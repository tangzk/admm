package com.intentmedia.admm;

import com.google.common.base.Optional;
import com.intentmedia.admm.AdmmIterationHelper;
import com.intentmedia.admm.AdmmIterationMapper;
import com.intentmedia.admm.AdmmIterationReducer;
import com.intentmedia.admm.AdmmMapperContext;
import com.intentmedia.admm.SignalInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.jetbrains.annotations.TestOnly;
import org.kohsuke.args4j.CmdLineParser;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import static com.intentmedia.admm.AdmmIterationHelper.fsDataInputStreamToString;
import static com.intentmedia.admm.AdmmIterationHelper.getFileLength;
import static com.intentmedia.admm.AdmmIterationHelper.jsonToMap;

public class AdmmOptimizerDriver extends Configured implements Tool {

    public static final String BETAS_PATH = "betas";
    private static final int DEFAULT_ADMM_ITERATIONS_MAX = 2;
    private static final float DEFAULT_REGULARIZATION_FACTOR = 0.000001f;
    private static final String ITERATION_PATH_PREFIX = "iteration_";
    private static final String ITERATION_FINAL_PREFIX = ITERATION_PATH_PREFIX + "final";

    @Override
    public int run(String[] args) throws Exception {
        AdmmOptimizerDriverArguments admmOptimizerDriverArguments = new AdmmOptimizerDriverArguments();
        new CmdLineParser(admmOptimizerDriverArguments).parseArgument(args);

        String inputDataLocation = admmOptimizerDriverArguments.getInputPath();
        String intermediateOutputLocation = "/tmp";
        URI finalOutputLocation = admmOptimizerDriverArguments.getOutputPath();
        int iterationsMaximum = Optional.fromNullable(admmOptimizerDriverArguments.getIterationsMaximum()).or(
                DEFAULT_ADMM_ITERATIONS_MAX);
        float regularizationFactor = Optional.fromNullable(admmOptimizerDriverArguments.getRegularizationFactor()).or(
                DEFAULT_REGULARIZATION_FACTOR);
        boolean addIntercept = Optional.fromNullable(admmOptimizerDriverArguments.getAddIntercept()).or(false);
        boolean regularizeIntercept = Optional.fromNullable(admmOptimizerDriverArguments.getRegularizeIntercept()).or(false);
        String columnsToExclude = Optional.fromNullable(admmOptimizerDriverArguments.getColumnsToExclude()).or("");

        long preStatus = 0;

        int iterationNumber = 0;
        JobConf conf = new JobConf(getConf(), AdmmOptimizerDriver.class);
        long curStatus = doAdmmIteration(conf,
                inputDataLocation,
                intermediateOutputLocation,
                iterationNumber,
                regularizationFactor,
                columnsToExclude,
                addIntercept,
                regularizeIntercept);
        boolean isFinalIteration = false;

        while (!convergedOrMaxed(curStatus, preStatus, iterationNumber, iterationsMaximum)) {
            iterationNumber++;
            preStatus = 0;
            conf = new JobConf(getConf(), AdmmOptimizerDriver.class);
            curStatus = doAdmmIteration(conf,
                    inputDataLocation,
                    intermediateOutputLocation,
                    iterationNumber,
                    regularizationFactor,
                    columnsToExclude,
                    addIntercept,
                    regularizeIntercept);
            if (convergedOrMaxed(curStatus, preStatus, iterationNumber, iterationsMaximum)) {
                isFinalIteration = true;
            }
            writeResultsToOutput(conf, intermediateOutputLocation, iterationNumber, finalOutputLocation, isFinalIteration);
        }

        return 0;
    }

    public long doAdmmIteration(JobConf conf,
                                String signalDataLocation,
                                String intermediateOutputLocation,
                                int iterationNumber,
                                float regularizationFactor,
                                String columnsToExclude,
                                boolean addIntercept,
                                boolean regularizeIntercept) throws IOException {
        FileSystem fs = FileSystem.get(conf);

        Path previousIntermediateOutputLocation =
                new Path(intermediateOutputLocation + ITERATION_PATH_PREFIX + (iterationNumber - 1));
        Path currentIntermediateOutputLocation = new Path(intermediateOutputLocation + ITERATION_PATH_PREFIX + iterationNumber);
        Path signalDataInputLocation = new Path(signalDataLocation);

        conf.setJobName("ADMM Optimizer " + iterationNumber);
        conf.set("mapred.child.java.opts", "-Xmx2g");
        conf.setInt("iteration.number", iterationNumber);
        conf.set("previous.intermediate.output.location", previousIntermediateOutputLocation.toString());
        conf.set("columns.to.exclude", columnsToExclude);
        conf.setBoolean("calculate.scaling.factors", false);
        conf.setBoolean("add.intercept", addIntercept);
        conf.setBoolean("regularize.intercept", regularizeIntercept);
        conf.setFloat("regularization.factor", regularizationFactor);

        conf.setMapperClass(AdmmIterationMapper.class);
        conf.setReducerClass(AdmmIterationReducer.class);
        conf.setMapOutputKeyClass(IntWritable.class);
        conf.setMapOutputValueClass(Text.class);
        conf.setOutputKeyClass(IntWritable.class);
        conf.setOutputValueClass(Text.class);
        conf.setInputFormat(SignalInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, signalDataInputLocation);
        if (fs.exists(currentIntermediateOutputLocation)) {
            fs.delete(currentIntermediateOutputLocation, true);
        }
        FileOutputFormat.setOutputPath(conf, currentIntermediateOutputLocation);

        RunningJob job = JobClient.runJob(conf);

        return job.getCounters().findCounter(AdmmIterationReducer.IterationCounter.ITERATION).getValue();
    }

    @TestOnly
    public void writeResultsToOutput(JobConf conf,
                                     String intermediateOutputLocation,
                                     int iterationNumber,
                                     URI finalOutputLocation,
                                     boolean isFinalIteration) throws IOException {
        Path intermediateOutputPath = new Path(intermediateOutputLocation + ITERATION_PATH_PREFIX + iterationNumber);

        FileSystem fs = FileSystem.get(conf);
        FileStatus[] hdfsFiles = fs.listStatus(intermediateOutputPath);
        Path[] hdfsFilePaths = FileUtil.stat2Paths(hdfsFiles);
        Path finalOutputPath;
        boolean betasToWrite = false;

        if (isFinalIteration) {
            finalOutputPath = new Path(finalOutputLocation.resolve(ITERATION_FINAL_PREFIX).toString());
            betasToWrite = true;
        }
        else {
            finalOutputPath = new Path(finalOutputLocation.resolve(ITERATION_PATH_PREFIX + iterationNumber).toString());
        }
        FileSystem s3fs = finalOutputPath.getFileSystem(conf);

        for (Path hdfsFilePath : hdfsFilePaths) {
            FileStatus fileStatus = fs.getFileStatus(hdfsFilePath);
            if (!fileStatus.isDir()) {
                FSDataInputStream in = fs.open(hdfsFilePath);
                Path finalOutputPathFull = getHdfsPath(finalOutputPath, hdfsFilePath.getName());

                if (s3fs.exists(finalOutputPathFull)) { // if the file exists on s3, delete it
                    s3fs.delete(finalOutputPathFull, true);
                }

                OutputStream out = s3fs.create(finalOutputPathFull);
                try {
                    IOUtils.copyBytes(in, out, conf, false);
                    if (betasToWrite) {
                        int inputSize = getFileLength(fs, hdfsFilePath);

                        if (inputSize > 0) {
                            in.seek(0);
                            writeBetas(in, inputSize, conf, finalOutputLocation);
                            betasToWrite = false;
                        }
                    }
                }
                finally {
                    in.close();
                    out.close();
                }
            }
        }
    }

    private void writeBetas(FSDataInputStream in, int inputSize, JobConf conf, URI finalOutputLocation)
            throws IOException {
        String jsonString = fsDataInputStreamToString(in, inputSize);
        String betasString = buildBetasString(jsonString);

        Path betasPath = new Path(finalOutputLocation.resolve(BETAS_PATH).toString());
        Path betasPathFull = getHdfsPath(betasPath, "part-00000");

        FileSystem s3fs = betasPath.getFileSystem(conf);

        InputStream inBetas = new ByteArrayInputStream(betasString.getBytes());
        OutputStream out = s3fs.create(betasPathFull);
        IOUtils.copyBytes(inBetas, out, conf, true);
    }

    @TestOnly
    public String buildBetasString(String jsonString) throws IOException {
        String betaString = jsonToMap(jsonString).values().iterator().next();

        AdmmMapperContext admmMapperContext = AdmmIterationHelper.jsonToAdmmMapperContext(betaString);

        double[] zInitials = admmMapperContext.getZInitial();
        StringBuilder outStringBuilder = new StringBuilder();

        for (int i = 0; i < zInitials.length; i++) {
            outStringBuilder.append(String.format("[%s]", zInitials[i]));

            if (i < zInitials.length - 1) {
                outStringBuilder.append(",");
            }
        }
        outStringBuilder.append("]");

        return outStringBuilder.toString();
    }

    private Path getHdfsPath(Path finalOutputPath, String name) {
        return new Path(finalOutputPath, name);
    }

    private boolean convergedOrMaxed(long curStatus, long preStatus, int iterationNumber, int iterationsMaximum) {
        return curStatus <= preStatus || iterationNumber >= iterationsMaximum;
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new AdmmOptimizerDriver(), args);
    }
}

