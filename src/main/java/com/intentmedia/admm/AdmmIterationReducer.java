package com.intentmedia.admm;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.logging.Logger;
import java.util.regex.Pattern;

import static com.intentmedia.admm.AdmmIterationHelper.admmMapperContextToJson;
import static com.intentmedia.admm.AdmmIterationHelper.jsonToAdmmReducerContext;
import static com.intentmedia.admm.AdmmIterationHelper.mapToJson;

public class AdmmIterationReducer extends MapReduceBase implements Reducer<IntWritable, Text, IntWritable, Text> {

    private static final double RHO_INCREMENT_MULTIPLIER = 2;
    private static final double RHO_DECREMENT_MULTIPLIER = 2;
    private static final double RHO_UPDATE_THRESHOLD = 10;
    private static final double THRESHOLD = 0.0001;
    private static final double SQUARE_ROOT_POWER = 0.5;
    private static final Logger LOG = Logger.getLogger(AdmmIterationReducer.class.getName());

    private static final IntWritable ZERO = new IntWritable(0);
    private static final Pattern KEY_VALUE_DELIMITER = Pattern.compile("::");

    private Map<String, String> splitToParameters = new HashMap<String, String>();
    private Map<String, AdmmReducerContext> reducerContexts;
    private Map<String, AdmmMapperContext> mapperContexts = new HashMap<String, AdmmMapperContext>();
    private int iteration;
    private boolean regularizeIntercept;

    public static enum IterationCounter {
        ITERATION
    }

    private class IterationDiagnostics {

        private final double rNorm;
        private final double sNorm;
        private final double primalObjectiveValue;

        IterationDiagnostics(double rNorm, double sNorm, double primalObjectiveValue) {
            this.rNorm = rNorm;
            this.sNorm = sNorm;
            this.primalObjectiveValue = primalObjectiveValue;
        }

        public String toString() {
            return "Objective Value: " + Double.toString(primalObjectiveValue) + " rNorm: " + Double
                    .toString(rNorm) + " sNorm:" + Double.toString(sNorm);
        }

        public double getRNorm() {
            return rNorm;
        }

        public double getSNorm() {
            return sNorm;
        }

        public double getPrimalObjectiveValue() {
            return primalObjectiveValue;
        }
    }

    @Override
    public void configure(JobConf job) {
        super.configure(job);
        iteration = Integer.parseInt(job.get("iteration.number"));
        regularizeIntercept = job.getBoolean("regularize.intercept", false);
    }

    @Override
    public void reduce(IntWritable key, Iterator<Text> values, OutputCollector<IntWritable, Text> output, Reporter reporter)
            throws IOException {

        reducerContexts = new HashMap<String, AdmmReducerContext>();
        while (values.hasNext()) {
            Text value = values.next();
            String[] result = KEY_VALUE_DELIMITER.split(value.toString());
            reducerContexts.put(result[0], jsonToAdmmReducerContext(result[1]));
            LOG.info("Iteration " + iteration + " Reducer Getting splitId " + result[0]);
        }

        setMapperValues();
        IterationDiagnostics iterationDiagnostics = setIterationDiagnostics();
        updateMapperRhosAndObjectives(iterationDiagnostics);
        if (iterationDiagnostics.getRNorm() > THRESHOLD || iterationDiagnostics.getSNorm() > THRESHOLD) {
            //if needs more iteration, increase the counter
            reporter.getCounter(IterationCounter.ITERATION).increment(1);
        }
        updateSharedParameters();

        output.collect(ZERO, new Text(mapToJson(splitToParameters)));
    }

    private IterationDiagnostics setIterationDiagnostics() {
        double rNorm = 0.0;
        double sNorm = 0.0;
        double primalObjectiveValue = 0.0;
        int reducerContextNumber = 0;
        int numFeatures = 0;
        double[] uMapperAverage = null;

        for (Map.Entry<String, AdmmReducerContext> entry : reducerContexts.entrySet()) {
            AdmmReducerContext reducerContext = entry.getValue();
            AdmmMapperContext mapperContext = mapperContexts.get(entry.getKey());

            numFeatures = reducerContext.getXUpdated().length;
            if (uMapperAverage == null) {
                uMapperAverage = new double[numFeatures];
            }

            double[] zMapper = mapperContext.getZInitial();
            if (reducerContextNumber == 0) {
                double[] zReducer = reducerContext.getZInitial();
                double rhoInitial = reducerContext.getRho();

                for (int featureNumber = 0; featureNumber < numFeatures; featureNumber++) {
                    sNorm += Math.pow(rhoInitial * (zMapper[featureNumber] - zReducer[featureNumber]), 2);
                }
            }

            primalObjectiveValue += reducerContext.getPrimalObjectiveValue();

            double[] xMapper = mapperContext.getXInitial();
            for (int featureNumber = 0; featureNumber < numFeatures; featureNumber++) {
                rNorm += Math.pow(xMapper[featureNumber] - zMapper[featureNumber], 2);
                uMapperAverage[featureNumber] += mapperContext.getUInitial()[featureNumber];
            }
            reducerContextNumber++;
        }

        for (int featureNumber = 0; featureNumber < numFeatures; featureNumber++) {
            uMapperAverage[featureNumber] /= numFeatures;
        }

        rNorm = Math.pow(rNorm, SQUARE_ROOT_POWER);
        sNorm = Math.pow(sNorm, SQUARE_ROOT_POWER);

        return new IterationDiagnostics(rNorm, sNorm, primalObjectiveValue);
    }

    private void updateMapperRhosAndObjectives(IterationDiagnostics iterationDiagnostics) {
        double reducerRho = 0.0; // get this from a reducer context
        for (AdmmReducerContext context : reducerContexts.values()) {
            reducerRho = context.getRho();
        }

        double rNorm = iterationDiagnostics.getRNorm();
        double sNorm = iterationDiagnostics.getSNorm();

        double newRho = reducerRho;
        if (rNorm > RHO_UPDATE_THRESHOLD * sNorm) {
            newRho = RHO_INCREMENT_MULTIPLIER * reducerRho;
        }
        else if (sNorm > RHO_UPDATE_THRESHOLD * rNorm) {
            newRho = reducerRho / RHO_DECREMENT_MULTIPLIER;
        }

        for (AdmmMapperContext context : mapperContexts.values()) {
            context.setRho(newRho);
            context.setPrimalObjectiveValue(iterationDiagnostics.getPrimalObjectiveValue());
            context.setRNorm(rNorm);
            context.setSNorm(sNorm);
        }
    }

    private void setMapperValues() {
        Map.Entry<double[], double[]> xuAverage = getXUAverageFromReducerContexts();
        double[] xAverage = xuAverage.getKey();
        double[] uAverage = xuAverage.getValue();
        int numMappers = reducerContexts.size();

        for (Map.Entry<String, AdmmReducerContext> entry : reducerContexts.entrySet()) {
            AdmmReducerContext context = entry.getValue();

            double[] zUpdated = getZUpdated(context, numMappers, xAverage, uAverage);
            double[] uUpdated = getUUpdated(context, zUpdated);

            AdmmMapperContext admmMapperContext =
                    new AdmmMapperContext(null, null, uUpdated, context.getXUpdated(), zUpdated, context.getRho(),
                            context.getLambdaValue(), -1, -1, -1);
            mapperContexts.put(entry.getKey(), admmMapperContext);
        }
    }

    private Map.Entry<double[], double[]> getXUAverageFromReducerContexts() {
        double[] xAverage = null;
        double[] uAverage = null;

        for (Map.Entry<String, AdmmReducerContext> entry : reducerContexts.entrySet()) {
            AdmmReducerContext context = entry.getValue();

            if (xAverage == null) {
                xAverage = new double[context.getXUpdated().length];
            }

            if (uAverage == null) {
                uAverage = new double[context.getUInitial().length];
            }

            for (int i = 0; i < context.getXUpdated().length; i++) {
                xAverage[i] += context.getXUpdated()[i];
                uAverage[i] += context.getUInitial()[i];
            }
        }

        int numMappers = reducerContexts.size();
        for (int i = 0; i < xAverage.length; i++) {
            xAverage[i] /= numMappers;
            uAverage[i] /= numMappers;
        }
        return new AbstractMap.SimpleEntry(xAverage, uAverage);
    }

    private double[] getZUpdated(AdmmReducerContext context, int numMappers, double[] xAverage, double[] uAverage) {
        double[] zUpdated = new double[context.getZInitial().length];
        double multiplier = (numMappers * context.getRho()) / (2 * context.getLambdaValue() + numMappers * context.getRho());
        for (int i = 0; i < context.getZInitial().length; i++) {
            if (i == 0 && !regularizeIntercept) { // do not regularize the intercept
                zUpdated[i] = xAverage[i] + uAverage[i];
            }
            else {
                zUpdated[i] = multiplier * (xAverage[i] + uAverage[i]);
            }
        }
        return zUpdated;
    }

    private double[] getUUpdated(AdmmReducerContext context, double[] zUpdated) {
        double[] uUpdated = new double[context.getUInitial().length];
        for (int i = 0; i < context.getXUpdated().length; i++) {
            uUpdated[i] = context.getUInitial()[i] + context.getXUpdated()[i] - zUpdated[i];
        }
        return uUpdated;
    }

    private void updateSharedParameters() throws IOException {
        for (String splitId : reducerContexts.keySet()) {
            AdmmMapperContext mapperContext;
            if (mapperContexts.containsKey(splitId)) {
                mapperContext = mapperContexts.get(splitId);
            }
            else {
                mapperContext = new AdmmMapperContext();
            }

            splitToParameters.put(splitId, admmMapperContextToJson(mapperContext));
            LOG.info("Iteration " + String.valueOf(iteration) + " Reducer Setting splitID " + splitId);
        }
    }
}
