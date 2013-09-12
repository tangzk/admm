package com.intentmedia.admm;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static com.intentmedia.admm.AdmmIterationHelper.admmMapperContextToJson;
import static com.intentmedia.admm.AdmmIterationHelper.admmReducerContextToJson;
import static com.intentmedia.admm.AdmmIterationHelper.mapToJson;
import static com.intentmedia.admm.TestHelper.reduceToOneValue;
import static org.junit.Assert.assertEquals;

public class AdmmIterationReducerTest {

    private static final Pattern KEY_VALUE_DELIMITER = Pattern.compile("::");
    private static final int NUMBER_OF_REDUCER_CONTEXTS = 2;
    private static final double[] DUMMY_STATIC_DOUBLE_ARRAY = {1.0, 2.0, 3.0, 4.0};
    private static final double[] X_UPDATED_0 = {-1.0, 5.0, 2.0, 1.0};
    private static final double[] X_UPDATED_1 = {3.0, 2.5, 2.0, -2.0};

    private static final double[] EXPECTED_Z_INITIAL = {1.0, 2.875, 2.5, 1.75};
    private static final double[] EXPECTED_U_INITIAL_0 = {-1.0, 4.125, 2.5, 3.25};
    private static final double[] EXPECTED_U_INITIAL_1 = {3.0, 1.625, 2.5, 0.25};
    private static final double EXPECTED_R_NORM = 3.952847075210474;
    private static final double EXPECTED_S_NORM = 6.9731628404906765;

    private static final double[] ZEROS_ARRAY = {0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0};
    private static final double[] X_UPDATED_SAMPLE = {-5.188985439925203, -0.1604979256483337, -2.169505472918433, 1.9893563708322046, -0.018388627665603553, 0.2417594012921784, 0.47769941269508875};
    private static final double[] EXPECTED_U_INITIAL_SAMPLE = {-1.556692514625979E-5, -4.814928127455653E-7, -6.508503384905495E-6, 5.968057161132023E-6, -5.5165772522985934E-8, 7.252767514898117E-7, 1.4330953682917258E-6};
    private static final double[] EXPECTED_X_UPDATED_SAMPLE = {-5.188985439925203, -0.1604979256483337, -2.169505472918433, 1.9893563708322046, -0.018388627665603553, 0.2417594012921784, 0.47769941269508875};
    private static final double[] EXPECTED_Z_INITIAL_SAMPLE = {-5.188975061975105, -0.1604976046531252, -2.1695011339161763, 1.9893523921274305, -0.01838859088842187, 0.24175891777434408, 0.47769845729817656};
    private static final double EXPECTED_LAMBDA_VALUE_SAMPLE = 9.999999974752427E-7;

    @Test
    public void testReducerWithMultipleMapOutputs() throws Exception {
        // initialize inputs
        List<Text> mapperResults = getMapperResults();
        String expectedJson = getExpectedJson();

        // initialize mocks
        // initialize subject
        AdmmIterationReducer subject = createSubject(Integer.toString(NUMBER_OF_REDUCER_CONTEXTS));

        // invoke target
        // assert
        Text result = reduceToOneValue(subject, new IntWritable(1), mapperResults);
        String resultString = result.toString();

        // verify
        assertEquals(expectedJson, resultString);
    }

    @Test
    public void testReducerWithSampleFileInput() throws Exception {
        // initialize inputs
        List<Text> mapperResults = getMapperResultsSample();
        String expectedJson = getExpectedJsonSample();

        // initialize mocks
        // initialize subject
        AdmmIterationReducer subject = createSubject(Integer.toString(1));

        // invoke target
        // assert
        Text result = reduceToOneValue(subject, new IntWritable(1), mapperResults);
        String resultString = result.toString();

        // verify
        assertEquals(expectedJson, resultString);
    }

    private AdmmIterationReducer createSubject(String numberOfMappersArgs) {
        JobConf jobConf = new JobConf();
        jobConf.set("mapred.map.tasks", numberOfMappersArgs);
        jobConf.set("iteration.number", "0");
        jobConf.setBoolean("regularize.intercept", true);

        AdmmIterationReducer subject = new AdmmIterationReducer();
        subject.configure(jobConf);

        return subject;
    }

    private List<Text> getMapperResultsSample() throws IOException {
        List<Text> reducerContextList = new ArrayList<Text>();
        AdmmReducerContext context0 = new AdmmReducerContext(
                ZEROS_ARRAY, ZEROS_ARRAY, X_UPDATED_SAMPLE, ZEROS_ARRAY,
                0.0, 1.0, 1e-6f
        );
        reducerContextList.add(new Text("0" + KEY_VALUE_DELIMITER +
                admmReducerContextToJson(context0)));
        return reducerContextList;
    }

    private List<Text> getMapperResults() throws IOException {
        List<Text> reducerContextList = new ArrayList<Text>();
        AdmmReducerContext context0 = new AdmmReducerContext(
                DUMMY_STATIC_DOUBLE_ARRAY, DUMMY_STATIC_DOUBLE_ARRAY, X_UPDATED_0,
                DUMMY_STATIC_DOUBLE_ARRAY, 1.0, 1.0, 1.0);
        reducerContextList.add(new Text("0" + KEY_VALUE_DELIMITER +
                admmReducerContextToJson(context0)));
        AdmmReducerContext context1 = new AdmmReducerContext(
                DUMMY_STATIC_DOUBLE_ARRAY, DUMMY_STATIC_DOUBLE_ARRAY, X_UPDATED_1,
                DUMMY_STATIC_DOUBLE_ARRAY, 1.0, 1.0, 1.0);
        reducerContextList.add(new Text("1" + KEY_VALUE_DELIMITER +
                admmReducerContextToJson(context1)));

        return reducerContextList;
    }

    private String getExpectedJsonSample() throws IOException {
        Map<String, String> outputMap = new HashMap<String, String>();
        AdmmMapperContext admmMapperContext0 = new AdmmMapperContext(null, null, EXPECTED_U_INITIAL_SAMPLE,
                EXPECTED_X_UPDATED_SAMPLE, EXPECTED_Z_INITIAL_SAMPLE, 0.6666666666666666, EXPECTED_LAMBDA_VALUE_SAMPLE, 0.0, 0.0, 5.991878230214353);
        outputMap.put("0", admmMapperContextToJson(admmMapperContext0));
        return mapToJson(outputMap);
    }

    private String getExpectedJson() throws IOException {
        Map<String, String> outputMap = new HashMap<String, String>();
        AdmmMapperContext admmMapperContext0 = new AdmmMapperContext(null, null, EXPECTED_U_INITIAL_0, X_UPDATED_0,
                EXPECTED_Z_INITIAL, 1.0, 1.0, 2.0, EXPECTED_R_NORM, EXPECTED_S_NORM);
        outputMap.put("0", admmMapperContextToJson(admmMapperContext0));
        AdmmMapperContext admmMapperContext1 = new AdmmMapperContext(null, null, EXPECTED_U_INITIAL_1, X_UPDATED_1,
                EXPECTED_Z_INITIAL, 1.0, 1.0, 2.0, EXPECTED_R_NORM, EXPECTED_S_NORM);
        outputMap.put("1", admmMapperContextToJson(admmMapperContext1));

        return mapToJson(outputMap);
    }
}