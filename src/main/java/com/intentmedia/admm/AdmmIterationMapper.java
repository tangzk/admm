package com.intentmedia.admm;

import com.intentmedia.bfgs.optimize.BFGS;
import com.intentmedia.bfgs.optimize.IOptimizer;
import com.intentmedia.bfgs.optimize.OptimizerParameters;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.intentmedia.admm.AdmmIterationHelper.*;

public class AdmmIterationMapper extends MapReduceBase
        implements Mapper<LongWritable, Text, IntWritable, Text> {

    private static final IntWritable ZERO = new IntWritable(0);
    private static final Logger LOG = Logger.getLogger(AdmmIterationMapper.class.getName());
    private static final float DEFAULT_REGULARIZATION_FACTOR = 0.000001f;
    private static final float DEFAULT_RHO = 1.0f;

    private int iteration;
    private FileSystem fs;
    private Map<String, String> splitToParameters;
    private Set<Integer> columnsToExclude;

    private OptimizerParameters optimizerParameters = new OptimizerParameters();
    private BFGS<LogisticL2DifferentiableFunction> bfgs = new BFGS<LogisticL2DifferentiableFunction>(optimizerParameters);
    private boolean addIntercept;
    private float regularizationFactor;
    private double rho;
    private String previousIntermediateOutputLocation;
    private Path previousIntermediateOutputLocationPath;

    private long mapStartTime;
    private long optimizationStartTime;
    private long mapEndTime;

    @Override
    public void configure(JobConf job) {
        iteration = Integer.parseInt(job.get("iteration.number"));
        String columnsToExcludeString = job.get("columns.to.exclude");
        columnsToExclude = getColumnsToExclude(columnsToExcludeString);
        addIntercept = job.getBoolean("add.intercept", false);
        rho = job.getFloat("rho", DEFAULT_RHO);
        regularizationFactor = job.getFloat("regularization.factor", DEFAULT_REGULARIZATION_FACTOR);
        previousIntermediateOutputLocation = job.get("previous.intermediate.output.location");
        previousIntermediateOutputLocationPath = new Path(previousIntermediateOutputLocation);

        try {
            fs = FileSystem.get(job);
        }
        catch (IOException e) {
            LOG.log(Level.FINE, e.toString());
        }

        splitToParameters = getSplitParameters();
    }

    protected Map<String, String> getSplitParameters() {
        return readParametersFromHdfs(fs, previousIntermediateOutputLocationPath, iteration);
    }

    @Override
    public void map(LongWritable key, Text value, OutputCollector<IntWritable, Text> output, Reporter reporter)
            throws IOException {
        mapStartTime = System.nanoTime();
        FileSplit split = (FileSplit) reporter.getInputSplit();
        String splitId = key.get() + "@" + split.getPath();
        splitId = removeIpFromHdfsFileName(splitId);

        double[][] inputSplitData = createMatrixFromDataString(value.toString(), columnsToExclude, addIntercept);

        AdmmMapperContext mapperContext;
        if (iteration == 0) {
            mapperContext = new AdmmMapperContext(inputSplitData, rho);
        }
        else {
            mapperContext = assembleMapperContextFromCache(inputSplitData, splitId);
        }
        optimizationStartTime = System.nanoTime();
        AdmmReducerContext reducerContext = localMapperOptimization(mapperContext);

        LOG.info("Iteration " + iteration + "Mapper outputting splitId " + splitId);
        output.collect(ZERO, new Text(splitId + "::" + admmReducerContextToJson(reducerContext)));
    }

    private AdmmReducerContext localMapperOptimization(AdmmMapperContext context) {
        LogisticL2DifferentiableFunction myFunction =
                new LogisticL2DifferentiableFunction(context.getA(),
                        context.getB(),
                        context.getRho(),
                        context.getUInitial(),
                        context.getZInitial());
        IOptimizer.Ctx optimizationContext = new IOptimizer.Ctx(context.getXInitial());
        bfgs.minimize(myFunction, optimizationContext);
        double primalObjectiveValue = myFunction.evaluatePrimalObjective(optimizationContext.m_optimumX);
        mapEndTime = System.nanoTime();

        return new AdmmReducerContext(context.getUInitial(),
                context.getXInitial(),
                optimizationContext.m_optimumX,
                context.getZInitial(),
                primalObjectiveValue,
                context.getRho(),
                regularizationFactor,
                mapStartTime,
                optimizationStartTime,
                mapEndTime);
    }

    private AdmmMapperContext assembleMapperContextFromCache(double[][] inputSplitData, String splitId) throws IOException {
        if (splitToParameters.containsKey(splitId)) {
            AdmmMapperContext preContext = jsonToAdmmMapperContext(splitToParameters.get(splitId));
            return new AdmmMapperContext(inputSplitData,
                    preContext.getUInitial(),
                    preContext.getXInitial(),
                    preContext.getZInitial(),
                    preContext.getRho(),
                    preContext.getLambdaValue(),
                    preContext.getPrimalObjectiveValue(),
                    preContext.getRNorm(),
                    preContext.getSNorm());
        }
        else {
            LOG.log(Level.FINE, "Key not found. Split ID: " + splitId + " Split Map: " + splitToParameters.toString());
            throw new IOException("Key not found.  Split ID: " + splitId + " Split Map: " + splitToParameters.toString());
        }
    }
}