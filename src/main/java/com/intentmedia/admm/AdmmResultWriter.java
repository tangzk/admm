package com.intentmedia.admm;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapred.JobConf;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public abstract class AdmmResultWriter {
    public abstract void write(JobConf conf,
                               FileSystem hdfs,
                               Path hdfsFilePath,
                               Path finalOutputPath) throws IOException;


    protected void getFSAndWriteFile(JobConf conf, InputStream in, Path finalOutputPathFull)
            throws IOException {
        FileSystem s3fs = finalOutputPathFull.getFileSystem(conf);
        if(s3fs.exists(finalOutputPathFull)) {
            s3fs.delete(finalOutputPathFull, true);
        }

        OutputStream out = s3fs.create(finalOutputPathFull);
        IOUtils.copyBytes(in, out, conf, true);

    }
}