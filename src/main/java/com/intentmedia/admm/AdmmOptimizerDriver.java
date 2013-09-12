package com.intentmedia.admm;

import com.google.common.base.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.kohsuke.args4j.CmdLineParser;

import java.io.IOException;
import java.net.URI;

public class AdmmOptimizerDriver extends Configured implements Tool {

    public static final String BETAS_PATH = "betas";
    private static final int DEFAULT_ADMM_ITERATIONS_MAX = 2;
    private static final float DEFAULT_REGULARIZATION_FACTOR = 0.000001f;
    private static final String S3_ITERATION_FOLDER_NAME = "iteration_";
    private static final String S3_FINAL_ITERATION_FOLDER_NAME = S3_ITERATION_FOLDER_NAME + "final";
    private static final String S3_STANDARD_ERROR_FOLDER_NAME = "standard-error";
    private static final String S3_BETAS_FOLDER_NAME = "betas";

    @Override
    public int run(String[] args) throws Exception {
        AdmmOptimizerDriverArguments admmOptimizerDriverArguments = new AdmmOptimizerDriverArguments();
        new CmdLineParser(admmOptimizerDriverArguments).parseArgument(args);

        String signalDataLocation = admmOptimizerDriverArguments.getSignalPath();
        String intermediateHdfsBaseString = "/tmp";
        URI finalOutputBaseUrl = admmOptimizerDriverArguments.getOutputPath();
        int iterationsMaximum = Optional.fromNullable(admmOptimizerDriverArguments.getIterationsMaximum()).or(
                DEFAULT_ADMM_ITERATIONS_MAX);
        float regularizationFactor = Optional.fromNullable(admmOptimizerDriverArguments.getRegularizationFactor()).or(
                DEFAULT_REGULARIZATION_FACTOR);
        boolean addIntercept = Optional.fromNullable(admmOptimizerDriverArguments.getAddIntercept()).or(false);
        boolean regularizeIntercept = Optional.fromNullable(admmOptimizerDriverArguments.getRegularizeIntercept()).or(false);
        String columnsToExclude = Optional.fromNullable(admmOptimizerDriverArguments.getColumnsToExclude()).or("");

        int iterationNumber = 0;
        boolean isFinalIteration = false;

        while (!isFinalIteration) {
            long preStatus = 0;
            JobConf conf = new JobConf(getConf(), AdmmOptimizerDriver.class);
            Path previousHdfsResultsPath = new Path(intermediateHdfsBaseString + S3_ITERATION_FOLDER_NAME + (iterationNumber - 1));
            Path currentHdfsResultsPath = new Path(intermediateHdfsBaseString + S3_ITERATION_FOLDER_NAME + iterationNumber);

            long curStatus = doAdmmIteration(conf,
                    previousHdfsResultsPath,
                    currentHdfsResultsPath,
                    signalDataLocation,
                    iterationNumber,
                    columnsToExclude,
                    addIntercept,
                    regularizeIntercept,
                    regularizationFactor);
            isFinalIteration = convergedOrMaxed(curStatus, preStatus, iterationNumber, iterationsMaximum);
            String s3IterationFolderName = getS3IterationFolderName(isFinalIteration, iterationNumber);
            printResultsToS3(conf, currentHdfsResultsPath, finalOutputBaseUrl, new AdmmResultWriterIteration(), s3IterationFolderName);

            if(isFinalIteration) {
                printResultsToS3(conf, currentHdfsResultsPath, finalOutputBaseUrl, new AdmmResultWriterBetas(),
                        S3_BETAS_FOLDER_NAME);
                JobConf stdErrConf = new JobConf(getConf(), AdmmOptimizerDriver.class);
                Path standardErrorHdfsPath = new Path(intermediateHdfsBaseString + S3_STANDARD_ERROR_FOLDER_NAME);
                doStandardErrorCalculation(
                        stdErrConf,
                        currentHdfsResultsPath,
                        standardErrorHdfsPath,
                        signalDataLocation,
                        iterationNumber,
                        columnsToExclude,
                        addIntercept,
                        regularizeIntercept,
                        regularizationFactor);
                printResultsToS3(stdErrConf, standardErrorHdfsPath, finalOutputBaseUrl, new AdmmResultWriterIteration(),
                        S3_STANDARD_ERROR_FOLDER_NAME);
            }
            iterationNumber++;
        }

        return 0;
    }

    private String getS3IterationFolderName(boolean isFinalIteration, int iterationNumber) {
        return (isFinalIteration) ? S3_FINAL_ITERATION_FOLDER_NAME : S3_ITERATION_FOLDER_NAME + iterationNumber;
    }

    public void printResultsToS3(JobConf conf, Path hdfsDirectoryPath, URI finalOutputBaseUrl,
                                 AdmmResultWriter admmResultWriter, String finalOutputFolderName) throws IOException {
        Path finalOutputPath = new Path(finalOutputBaseUrl.resolve(finalOutputFolderName).toString());
        HdfsToS3ResultsWriter hdfsToS3ResultsWriter = new HdfsToS3ResultsWriter(conf, hdfsDirectoryPath,
                admmResultWriter, finalOutputPath);
        hdfsToS3ResultsWriter.writeToS3();
    }

    public void doStandardErrorCalculation(JobConf conf,
                                           Path currentHdfsPath,
                                           Path standardErrorHdfsPath,
                                           String signalDataLocation,
                                           int iterationNumber,
                                           String columnsToExclude,
                                           boolean addIntercept,
                                           boolean regularizeIntercept,
                                           float regularizationFactor) throws IOException {
        Path signalDataInputLocation = new Path(signalDataLocation);

        // No addIntercept option as it would be added in the intermediate data by the Admm iterations.
        conf.setJobName("ADMM Standard Errors");
        conf.set("mapred.child.java.opts", "-Xmx2g");
        conf.set("previous.intermediate.output.location", currentHdfsPath.toString());
        conf.set("columns.to.exclude", columnsToExclude);
        conf.setInt("iteration.number", iterationNumber);
        conf.setBoolean("add.intercept", addIntercept);
        conf.setBoolean("regularize.intercept", regularizeIntercept);
        conf.setFloat("regularization.factor", regularizationFactor);

        conf.setMapperClass(AdmmStandardErrorsMapper.class);
        conf.setReducerClass(AdmmStandardErrorsReducer.class);
        conf.setMapOutputKeyClass(IntWritable.class);
        conf.setMapOutputValueClass(Text.class);
        conf.setOutputKeyClass(IntWritable.class);
        conf.setOutputValueClass(Text.class);
        conf.setInputFormat(SignalInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, signalDataInputLocation);
        FileOutputFormat.setOutputPath(conf, standardErrorHdfsPath);

        JobClient.runJob(conf);
    }

    public long doAdmmIteration(JobConf conf,
                                Path previousHdfsPath,
                                Path currentHdfsPath,
                                String signalDataLocation,
                                int iterationNumber,
                                String columnsToExclude,
                                boolean addIntercept,
                                boolean regularizeIntercept,
                                float regularizationFactor) throws IOException {
        Path signalDataInputLocation = new Path(signalDataLocation);

        conf.setJobName("ADMM Optimizer " + iterationNumber);
        conf.set("mapred.child.java.opts", "-Xmx2g");
        conf.set("previous.intermediate.output.location", previousHdfsPath.toString());
        conf.setInt("iteration.number", iterationNumber);
        conf.set("columns.to.exclude", columnsToExclude);
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
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(currentHdfsPath)) {
            fs.delete(currentHdfsPath, true);
        }
        FileOutputFormat.setOutputPath(conf, currentHdfsPath);

        RunningJob job = JobClient.runJob(conf);

        return job.getCounters().findCounter(AdmmIterationReducer.IterationCounter.ITERATION).getValue();
    }

    private boolean convergedOrMaxed(long curStatus, long preStatus, int iterationNumber, int iterationsMaximum) {
        return curStatus <= preStatus || iterationNumber >= iterationsMaximum;
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new AdmmOptimizerDriver(), args);
    }
}
