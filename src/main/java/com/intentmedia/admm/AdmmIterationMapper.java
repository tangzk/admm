package com.intentmedia.admm;

import com.intentmedia.bfgs.optimize.BFGS;
import com.intentmedia.bfgs.optimize.IOptimizer;
import com.intentmedia.bfgs.optimize.OptimizerParameters;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.intentmedia.bfgs.AdmmIterationHelper.admmReducerContextToJson;
import static com.intentmedia.bfgs.AdmmIterationHelper.createMatrixFromDataString;
import static com.intentmedia.bfgs.AdmmIterationHelper.fsDataInputStreamToString;
import static com.intentmedia.bfgs.AdmmIterationHelper.getColumnsToExclude;
import static com.intentmedia.bfgs.AdmmIterationHelper.getFileLength;
import static com.intentmedia.bfgs.AdmmIterationHelper.jsonToAdmmMapperContext;
import static com.intentmedia.bfgs.AdmmIterationHelper.jsonToMap;
import static com.intentmedia.bfgs.AdmmIterationHelper.removeIpFromHdfsFileName;

public class AdmmIterationMapper extends MapReduceBase
        implements Mapper<LongWritable, Text, IntWritable, Text> {

    private static final IntWritable ZERO = new IntWritable(0);
    private static final Logger LOG = Logger.getLogger(AdmmIterationMapper.class.getName());
    private static final float DEFAULT_REGULARIZATION_FACTOR = 0.000001f;

    private int iteration;
    private FileSystem fs;
    private Map<String, String> splitToParameters;
    private Set<Integer> columnsToExclude;

    private OptimizerParameters optimizerParameters = new OptimizerParameters();
    private BFGS<LogisticL2DifferentiableFunction> bfgs = new BFGS<LogisticL2DifferentiableFunction>(optimizerParameters);
    private boolean addIntercept;
    private float regularizationFactor;
    private String previousIntermediateOutputLocation;
    private Path previousIntermediateOutputLocationPath;

    @Override
    public void configure(JobConf job) {
        iteration = Integer.parseInt(job.get("iteration.number"));
        String columnsToExcludeString = job.get("columns.to.exclude");
        columnsToExclude = getColumnsToExclude(columnsToExcludeString);
        addIntercept = job.getBoolean("add.intercept", false);
        regularizationFactor = job.getFloat("regularization.factor", DEFAULT_REGULARIZATION_FACTOR);
        previousIntermediateOutputLocation = job.get("previous.intermediate.output.location");
        previousIntermediateOutputLocationPath = new Path(previousIntermediateOutputLocation);

        try {
            fs = FileSystem.get(job);
        }
        catch (IOException e) {
            LOG.log(Level.FINE, e.toString());
        }

        readParametersFromHdfs(job);
    }

    public void readParametersFromHdfs(JobConf job) {
        try {
            splitToParameters = new HashMap<String, String>();
            if (iteration > 0 && fs.exists(previousIntermediateOutputLocationPath)) {
                FileSystem fs = FileSystem.get(job);
                FileStatus[] fileStatuses = fs.listStatus(previousIntermediateOutputLocationPath);
                for (FileStatus fileStatus : fileStatuses) {
                    Path thisFilePath = fileStatus.getPath();
                    if (!thisFilePath.getName().contains("_SUCCESS") && !thisFilePath.getName().contains("_logs")) {
                        FSDataInputStream in = fs.open(thisFilePath);
                        int inputSize = getFileLength(fs, thisFilePath);
                        if (inputSize > 0) {
                            String value = fsDataInputStreamToString(in, inputSize);
                            Map<String, String> additionalSplitToParameters = jsonToMap(value);
                            splitToParameters.putAll(additionalSplitToParameters);
                        }
                    }
                }
            }
        }
        catch (IOException e) {
            LOG.log(Level.FINE, e.toString());
        }
    }

    @Override
    public void map(LongWritable key, Text value, OutputCollector<IntWritable, Text> output, Reporter reporter)
            throws IOException {
        FileSplit split = (FileSplit) reporter.getInputSplit();
        String splitId = key.get() + "@" + split.getPath();
        splitId = removeIpFromHdfsFileName(splitId);

        double[][] inputSplitData = createMatrixFromDataString(value.toString(), columnsToExclude, addIntercept);

        AdmmMapperContext mapperContext;
        if (iteration == 0) {
            mapperContext = new AdmmMapperContext(inputSplitData);
        }
        else {
            mapperContext = assembleMapperContextFromCache(inputSplitData, splitId);
        }
        AdmmReducerContext reducerContext = localMapperOptimization(mapperContext, splitId);

        LOG.info("Iteration " + iteration + "Mapper outputting splitId " + splitId);
        output.collect(ZERO, new Text(splitId + "::" + admmReducerContextToJson(reducerContext)));
    }

    private AdmmReducerContext localMapperOptimization(AdmmMapperContext context, String splitId) {
        LogisticL2DifferentiableFunction myFunction =
                new LogisticL2DifferentiableFunction(context.getA(),
                        context.getB(),
                        context.getRho(),
                        context.getUInitial(),
                        context.getZInitial());
        IOptimizer.Ctx optimizationContext = new IOptimizer.Ctx(context.getXInitial());
        bfgs.minimize(myFunction, optimizationContext);
        double objectivePrimalValue = myFunction.evaluateObjectivePrimal(optimizationContext.m_optimumX);
        return new AdmmReducerContext(context.getUInitial(),
                optimizationContext.m_optimumX,
                context.getZInitial(),
                objectivePrimalValue,
                context.getRho(),
                regularizationFactor);
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

