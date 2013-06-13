package com.intentmedia.admm;

import org.apache.hadoop.mapred.JobConf;
import org.junit.Test;

import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class AdmmOptimizerDriverTest {

    @Test
    public void testWriteResultsToOutput() throws Exception {
        // initialize inputs
        JobConf conf = new JobConf(AdmmOptimizerDriver.class);
        URI uri = URI.create("");

        // initialize mocks
        // initialize subject
        AdmmOptimizerDriver subject = new AdmmOptimizerDriver();

        // invoke target
        subject.writeResultsToOutput(conf, 1, uri, false);
        subject.writeResultsToOutput(conf, 1, uri, true);

        // assert
        // verify
    }

    @Test
    public void testBuildBetasString() throws Exception {
        // initialize inputs
        List<Double> zInitialList = Collections.unmodifiableList(Arrays.asList(
                0.04304983847577377,
                0.0,
                -0.1650450935084053,
                -0.3164945650455424));

        StringBuilder zBuilder = new StringBuilder();
        StringBuilder outputBuilder = new StringBuilder();

        for (int i = 0; i < zInitialList.size(); i++) {
            zBuilder.append(zInitialList.get(i));
            outputBuilder.append(String.format("[%s]", zInitialList.get(i)));

            if (i < zInitialList.size() - 1) {
                zBuilder.append(",");
                outputBuilder.append(",");
            }
        }

        outputBuilder.append("]");
        String expectedOutput = outputBuilder.toString();

        String jsonString =
                "{\"0@s3n://bucket/path/training.gz/part-m-0001.gz\":\"{\\\"a\\\":null,\\\"b\\\":null,\\\"uInitial\\\":[-0.06339187986530806],\\\"zInitial\\\":[" +
                        zBuilder.toString() +
                        "],\\\"rho\\\":32.0,\\\"lambdaValue\\\":0.0010000000474974513,\\\"primalObjectiveValue\\\":5728699.079705867,\\\"rNorm\\\":7.930098254024272,\\\"sNorm\\\":1.0488503599825136}\"}";

        // initialize mocks
        // initialize subject
        AdmmOptimizerDriver subject = new AdmmOptimizerDriver();

        // invoke target

        String returnValue = subject.buildBetasString(jsonString);

        // assert
        assertEquals(expectedOutput, returnValue);
        // verify
    }
}
