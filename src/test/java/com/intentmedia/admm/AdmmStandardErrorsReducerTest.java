package com.intentmedia.admm;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.intentmedia.admm.AdmmIterationHelper.admmStandardErrorReducerContextToJson;
import static com.intentmedia.admm.AdmmIterationHelper.arrayToJson;
import static com.intentmedia.admm.TestHelper.reduceToOneValue;
import static org.junit.Assert.assertEquals;

public class AdmmStandardErrorsReducerTest {
    private static final String KEY_VALUE_DELIMITER = "::";
    private static final double[][] XWX_MATRIX_0 = {{1.5, 0.0, 0.1}, {0.0, 1.0, 0.0}, {0.1, 0.0, 1.0}};
    private static final double[][] XWX_MATRIX_1 = {{5.0, 2.0, 1.0}, {2.0, 3.0, 0.0}, {1.0, 0.0, 2.0}};
    private static final double LAMBDA_VALUE = 0.5;
    private static final double[] EXPECTED_STANDARD_ERRORS = {0.3024701022281359, 0.3339442267999535, 0.3454380896980175};
    private static final int NUMBER_TRAINING_EXAMPLES = 1;
    private static final double[][] XWX_MATRIX_SAMPLE = {{53.76813981320403, -93.05472384958634, 42.144212149567956, 95.99222757670705, -3.1223365028878605, 10.18846647442964, 38.62849128766009},
            {-93.05472384958631, 409.5151027986737, -65.08794369247198, -157.83634015467845, 7.9693897925522466, -20.749865400158356, -66.66605745147577},
            {42.14421214956797, -65.08794369247207, 85.85416324788306, 128.0791011465539, 0.7897265624627079, 7.3775661981834215, 31.13359173100496},
            {95.99222757670702, -157.83634015467854, 128.07910114655388, 224.3419566693007, -2.2838330514063876, 17.47937516296623, 69.76380507025026},
            {-3.1223365028878605, 7.969389792552236, 0.7897265624627076, -2.2838330514063845, 52.373511436250105, -0.974676900896977, -2.4494620874267197},
            {10.188466474429617, -20.749865400158384, 7.377566198183425, 17.47937516296626, -0.9746769008969749, 57.20502803607444, 6.500121920060987},
            {38.62849128766012, -66.66605745147575, 31.13359173100498, 69.76380507025017, -2.4494620874267197, 6.5001219200609865, 38.62849128766012}};
    private static final double LAMBDA_VALUE_SAMPLE = 9.999999974752427E-10;
    private static final double[] EXPECTED_STANDARD_ERRORS_SAMPLE = {3.0753879459719724, 0.06384482696364942, 3.0377635563600456, 3.034338339096756, 0.13874448527877026, 0.13474699692331277, 0.3040146276637223};
    private static final int NUMBER_TRAINING_EXAMPLES_SAMPLE = 1000;

    @Test
    public void testStandardErrorsMapper() throws Exception {
        // initialize inputs
        List<Text> mapperResults = getMapperResults();
        String expectedJson = getExpectedJson();

        // initialize mocks
        // initialize subject
        AdmmStandardErrorsReducer subject = createSubject();

        // invoke target
        // assert
        Text result = reduceToOneValue(subject, new IntWritable(1), mapperResults);
        String resultString = result.toString();

        // verify
        assertEquals(expectedJson, resultString);
    }

    @Test
    public void testWithSampleFileInput() throws Exception {
        // initialize inputs
        List<Text> mapperResults = getMapperResultsSample();
        String expectedJson = getExpectedJsonSample();

        // initialize mocks
        // initialize subject
        AdmmStandardErrorsReducer subject = createSubject();

        // invoke target
        // assert
        Text result = reduceToOneValue(subject, new IntWritable(1), mapperResults);
        String resultString = result.toString();

        // verify
        assertEquals(expectedJson, resultString);
    }

    private AdmmStandardErrorsReducer createSubject() {
        JobConf jobConf = new JobConf();
        AdmmStandardErrorsReducer subject = new AdmmStandardErrorsReducer();
        subject.configure(jobConf);

        return subject;
    }

    private List<Text> getMapperResults() throws IOException {
        List<Text> reducerContextList = new ArrayList<Text>();
        AdmmStandardErrorsReducerContext context0 = new AdmmStandardErrorsReducerContext(
                XWX_MATRIX_0, LAMBDA_VALUE, NUMBER_TRAINING_EXAMPLES);
        reducerContextList.add(new Text("0" + KEY_VALUE_DELIMITER +
                admmStandardErrorReducerContextToJson(context0)));
        AdmmStandardErrorsReducerContext context1 = new AdmmStandardErrorsReducerContext(
                XWX_MATRIX_1, LAMBDA_VALUE, NUMBER_TRAINING_EXAMPLES);
        reducerContextList.add(new Text("1" + KEY_VALUE_DELIMITER +
                admmStandardErrorReducerContextToJson(context1)));

        return reducerContextList;
    }

    private List<Text> getMapperResultsSample() throws IOException {
        List<Text> reducerContextList = new ArrayList<Text>();
        AdmmStandardErrorsReducerContext context0 = new AdmmStandardErrorsReducerContext(
                XWX_MATRIX_SAMPLE, LAMBDA_VALUE_SAMPLE, NUMBER_TRAINING_EXAMPLES_SAMPLE);
        reducerContextList.add(new Text("0" + KEY_VALUE_DELIMITER +
                admmStandardErrorReducerContextToJson(context0)));

        return reducerContextList;
    }

    private String getExpectedJson() throws IOException {
        return arrayToJson(EXPECTED_STANDARD_ERRORS);
    }

    private String getExpectedJsonSample() throws IOException {
        return arrayToJson(EXPECTED_STANDARD_ERRORS_SAMPLE);
    }
}
