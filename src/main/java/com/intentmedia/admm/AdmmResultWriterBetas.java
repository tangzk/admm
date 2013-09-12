package com.intentmedia.admm;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.jetbrains.annotations.TestOnly;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import static com.intentmedia.admm.AdmmIterationHelper.*;

public class AdmmResultWriterBetas extends AdmmResultWriter {
    @Override
    public void write(JobConf conf,
                      FileSystem hdfs,
                      Path hdfsFilePath,
                      Path finalOutputPath) throws IOException {
        int inputSize = getFileLength(hdfs, hdfsFilePath);

        if (inputSize > 0) {
            FSDataInputStream in = hdfs.open(hdfsFilePath);
            in.seek(0);
            writeBetas(in, inputSize, conf, hdfsFilePath, finalOutputPath);
        }
    }

    private void writeBetas(FSDataInputStream in, int inputSize, JobConf conf,
                            Path hdfsFilePath, Path finalOutputPath)
            throws IOException {
        String jsonString = fsDataInputStreamToString(in, inputSize);
        String betasString = buildBetasString(jsonString);
        InputStream inBetas = new ByteArrayInputStream(betasString.getBytes());

        Path betasPathFull = new Path(finalOutputPath, hdfsFilePath.getName());

        getFSAndWriteFile(conf, inBetas, betasPathFull);
    }

    @TestOnly
    public String buildBetasString(String jsonString) throws IOException {
        String betaString = jsonToMap(jsonString).values().iterator().next();

        AdmmMapperContext admmMapperContext = AdmmIterationHelper.jsonToAdmmMapperContext(betaString);

        double[] zInitials = admmMapperContext.getZInitial();
        StringBuilder outStringBuilder = new StringBuilder();
        outStringBuilder.append("[");

        for (int i = 0; i < zInitials.length; i++) {
            outStringBuilder.append(String.format("[%s]", zInitials[i]));

            if (i < zInitials.length - 1) {
                outStringBuilder.append(",");
            }
        }
        outStringBuilder.append("]");

        return outStringBuilder.toString();
    }

}
