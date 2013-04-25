package com.intentmedia.admm;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

public class SignalInputFormat extends FileInputFormat<LongWritable, Text>
        implements JobConfigurable {

    private CompressionCodecFactory compressionCodecs = null;

    /*
    Implement this per JobConfigurable interface
     */
    @Override()
    public void configure(JobConf conf) {
        compressionCodecs = new CompressionCodecFactory(conf);
    }

    /*
    Override this so that files are not split up - each mapper should work on a whole file
     */
    @Override
    protected boolean isSplitable(FileSystem fs, Path file) {
        return false;
    }

    /*
    Return a record reader that will cause each map task to operate on an entire file (and not just a single line)
     */
    @Override
    public RecordReader<LongWritable, Text> getRecordReader(
            InputSplit genericSplit, JobConf job,
            Reporter reporter)
            throws IOException {

        reporter.setStatus(genericSplit.toString());
        return new WholeFileRecordReader(job, (FileSplit) genericSplit);
    }
}

