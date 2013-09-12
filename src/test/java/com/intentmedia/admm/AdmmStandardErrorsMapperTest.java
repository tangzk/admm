package com.intentmedia.admm;

import com.intentmedia.admm.helpers.BFGSTestHelper;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.intentmedia.admm.AdmmIterationHelper.admmMapperContextToJson;
import static com.intentmedia.admm.AdmmIterationHelper.admmStandardErrorReducerContextToJson;
import static com.intentmedia.admm.TestHelper.assertMaps;

public class AdmmStandardErrorsMapperTest {

    private static final String SAMPLE_DATA_FILE = BFGSTestHelper.BASE_PATH + "logreg_training";
    private static final IntWritable ZERO = new IntWritable(0);
    private static final int ITERATION_NUMBER = 50;
    private static final String PREVIOUS_INTERMEDIATE_OUTPUT_LOCATION = "dummy-loc";
    private static final int NUM_TRAINING_EXAMPLES = 5;
    private static final int NUM_FEATURES = 4;
    private static final String INPUT_SPLIT_DATA = "1.0\t1.0\t2.0\t1.0\t1.0\n0.0\t1.0\t2.0\t1.0\t0.0\n5.0\t-1.0\t1.0\t5.0\t1.0\n5.0\t1.5\t-1.0\t5.0\t0.0\n-0.5\t1.5\t2.5\t3.5\t1.0";
    private static final String SPLIT_ID = "0@somefile";
    private static final double[] STORED_Z_INITIAL = {0.5, -3.0, -1.5, 2.0};
    private static final double STORED_LAMBDA_VALUE = 0.5;
    private static final String KEY_VALUE_DELIMITER = "::";
    private static final double[][] XWX_MATRIX_0 = {{0.06763144071954585, -0.08284966811804867, -0.12989706281952818, -0.23066146342111984},
            {-0.08284966811804867, 0.3818644701861057, 0.651417562984136, 0.8296917505172394},
            {-0.12989706281952818, 0.651417562984136, 1.116703917964576, 1.3968928703103825},
            {-0.23066146342111984, 0.8296917505172395, 1.3968928703103827, 1.8750515717768312}};
    private static final double[][] XWX_MATRIX_SAMPLE = {{53.76813981320403, -93.05472384958634, 42.144212149567956, 95.99222757670705, -3.1223365028878605, 10.18846647442964, 38.62849128766009},
            {-93.05472384958631, 409.5151027986737, -65.08794369247198, -157.83634015467845, 7.9693897925522466, -20.749865400158356, -66.66605745147577},
            {42.14421214956797, -65.08794369247207, 85.85416324788306, 128.0791011465539, 0.7897265624627079, 7.3775661981834215, 31.13359173100496},
            {95.99222757670702, -157.83634015467854, 128.07910114655388, 224.3419566693007, -2.2838330514063876, 17.47937516296623, 69.76380507025026},
            {-3.1223365028878605, 7.969389792552236, 0.7897265624627076, -2.2838330514063845, 52.373511436250105, -0.974676900896977, -2.4494620874267197},
            {10.188466474429617, -20.749865400158384, 7.377566198183425, 17.47937516296626, -0.9746769008969749, 57.20502803607444, 6.500121920060987},
            {38.62849128766012, -66.66605745147575, 31.13359173100498, 69.76380507025017, -2.4494620874267197, 6.5001219200609865, 38.62849128766012}};
    private static final double STORED_LAMBDA_VALUE_SAMPLE = 9.999999974752427E-7;
    private static final double[] STORED_Z_INITIAL_SAMPLE = {-5.188975061975105, -0.1604976046531252, -2.1695011339161763, 1.9893523921274305, -0.01838859088842187, 0.24175891777434408, 0.47769845729817656};
    private static final int NUM_TRAINING_EXAMPLES_SAMPLE = 1000;
    private static final int NUM_FEATURES_SAMPLE = 7;

    @Test
    public void testStandardErrorsMapper() throws Exception {
        // initialize inputs
        LongWritable mapperInputKey = new LongWritable(0);
        List<Text> mapperInputValues = getMapperInputValues();
        List<IntWritable> mapperOutputKeys = getMapperOutputKeys();
        List<Text> mapperOutputValues = getMapperOutputValues();

        // initialize mocks
        // initialize subject
        TestableAdmmStandardErrorsMapper subject =
                createSubject(new TestableAdmmStandardErrorsMapper());

        // invoke target
        // assert
        assertMaps(subject, mapperInputKey, mapperInputValues, mapperOutputKeys, mapperOutputValues);

        // verify
    }

    @Test
    public void testWithSampleFileInput() throws Exception {
        // initialize inputs
        LongWritable mapperInputKey = new LongWritable(0);
        List<Text> mapperInputValues = getMapperInputValuesSample();
        List<IntWritable> mapperOutputKeys = getMapperOutputKeys();
        List<Text> mapperOutputValues = getMapperOutputValuesSample();

        // initialize mocks
        // initialize subject
        TestableAdmmStandardErrorsMapperSample subject =
                createSubject(new TestableAdmmStandardErrorsMapperSample());

        // invoke target
        // assert
        assertMaps(subject, mapperInputKey, mapperInputValues, mapperOutputKeys, mapperOutputValues);

        // verify
    }

    private <T extends AdmmStandardErrorsMapper> T createSubject(T result) {
        JobConf jobConf = new JobConf();
        jobConf.setInt("iteration.number", ITERATION_NUMBER);
        jobConf.set("columns.to.exclude", "");
        jobConf.set("previous.intermediate.output.location", PREVIOUS_INTERMEDIATE_OUTPUT_LOCATION);

        result.configure(jobConf);

        return result;
    }

    private List<Text> getMapperInputValues() {
        List<Text> mapperInputValues = new ArrayList<Text>();
        mapperInputValues.add(new Text(INPUT_SPLIT_DATA));
        return mapperInputValues;
    }

    private List<Text> getMapperInputValuesSample() throws IOException {
        List<Text> mapperInputValues = new ArrayList<Text>();
        File sampleAdmmDataFile = new File(SAMPLE_DATA_FILE);
        String sampleAdmmData = FileUtils.readFileToString(sampleAdmmDataFile);
        sampleAdmmData = addInterceptToSampleData(sampleAdmmData);
        mapperInputValues.add(new Text(sampleAdmmData));
        return mapperInputValues;
    }

    private String addInterceptToSampleData(String admmSampleData) {
        StringBuilder result = new StringBuilder("1.0\t");
        for (int i = 0; i < admmSampleData.length(); i++) {
            if (admmSampleData.charAt(i) == '\n') {
                result.append(admmSampleData.charAt(i) + "1\t");
            } else {
                result.append(admmSampleData.charAt(i));
            }
        }
        return result.substring(0, result.length() - 2);
    }

    private List<IntWritable> getMapperOutputKeys() {
        List<IntWritable> mapperOutputKeys = new ArrayList<IntWritable>();
        mapperOutputKeys.add(ZERO);
        return mapperOutputKeys;
    }

    private List<Text> getMapperOutputValues() throws IOException {
        List<Text> mapperOutputValues = new ArrayList<Text>();
        AdmmStandardErrorsReducerContext context0 = new AdmmStandardErrorsReducerContext(
                XWX_MATRIX_0, STORED_LAMBDA_VALUE, NUM_TRAINING_EXAMPLES);
        mapperOutputValues.add(new Text(SPLIT_ID + KEY_VALUE_DELIMITER +
                admmStandardErrorReducerContextToJson(context0)));

        return mapperOutputValues;
    }

    private List<Text> getMapperOutputValuesSample() throws IOException {
        List<Text> mapperOutputValues = new ArrayList<Text>();
        AdmmStandardErrorsReducerContext context0 = new AdmmStandardErrorsReducerContext(
                XWX_MATRIX_SAMPLE, STORED_LAMBDA_VALUE_SAMPLE, NUM_TRAINING_EXAMPLES_SAMPLE);
        mapperOutputValues.add(new Text(SPLIT_ID + KEY_VALUE_DELIMITER +
                admmStandardErrorReducerContextToJson(context0)));

        return mapperOutputValues;
    }

    private class TestableAdmmStandardErrorsMapper extends AdmmStandardErrorsMapper {
        @Override
        protected Map<String, String> getSplitParameters() {
            Map<String, String> splitParameters = new HashMap<String, String>();
            try {
                splitParameters.put(SPLIT_ID, admmMapperContextToJson(new AdmmMapperContext(
                        new double[NUM_TRAINING_EXAMPLES][NUM_FEATURES],
                        new double[NUM_FEATURES],
                        new double[NUM_FEATURES],
                        STORED_Z_INITIAL,
                        0.0,
                        STORED_LAMBDA_VALUE,
                        0.0,
                        0.0,
                        0.0
                )));
            } catch (IOException ignored) {

            }
            return splitParameters;
        }
    }

    private class TestableAdmmStandardErrorsMapperSample extends AdmmStandardErrorsMapper {
        @Override
        protected Map<String, String> getSplitParameters() {
            Map<String, String> splitParameters = new HashMap<String, String>();
            try {
                splitParameters.put(SPLIT_ID, admmMapperContextToJson(new AdmmMapperContext(
                        new double[NUM_TRAINING_EXAMPLES_SAMPLE][NUM_FEATURES_SAMPLE],
                        new double[NUM_FEATURES_SAMPLE],
                        new double[NUM_FEATURES_SAMPLE],
                        STORED_Z_INITIAL_SAMPLE,
                        0.0,
                        STORED_LAMBDA_VALUE_SAMPLE,
                        0.0,
                        0.0,
                        0.0
                )));
            } catch (IOException ignored) {

            }
            return splitParameters;
        }
    }
}