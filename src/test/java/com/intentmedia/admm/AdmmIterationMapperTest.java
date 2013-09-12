package com.intentmedia.admm;

import com.intentmedia.admm.helpers.BFGSTestHelper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.intentmedia.admm.AdmmIterationHelper.admmReducerContextToJson;
import static com.intentmedia.admm.TestHelper.assertMaps;

public class AdmmIterationMapperTest {
    private static final String SPLIT_ID = "0@somefile";
    private static final IntWritable ZERO = new IntWritable(0);
    private static final String KEY_VALUE_DELMITER = "::";
    private static final int NUMBER_OF_FEATURES = 7;
    private static final double[] EXPECTED_U_INITIAL = {0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0};
    private static final double[] EXPECTED_X_INITIAL = {0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0};
    private static final double[] EXPECTED_X_UPDATED = {-5.188985439925203, -0.1604979256483337, -2.169505472918433, 1.9893563708322046, -0.018388627665603553, 0.2417594012921784, 0.47769941269508875};
    private static final double[] EXPECTED_Z_INITIAL = new double[NUMBER_OF_FEATURES];
    private static final double EXPECTED_PRIMAL_OBJECTIVE_VALUE = 0.21405053339257565;
    private static final float EXPECTED_RHO = 0f;
    private static final float EXPECTED_LAMBDA_VALUE = 1e-6f;
    private static final int ITERATION_NUMBER = 0;
    private static final String PREVIOUS_INTERMEDIATE_OUTPUT_LOCATION = "/tmp";

    @Test
    public void testWithSampleFileInput() throws Exception {
        // initialize inputs
        LongWritable mapperInputKey = new LongWritable(0);
        List<Text> mapperInputValues = BFGSTestHelper.getMapperInputValues();
        List<IntWritable> mapperOutputKeys = getMapperOutputKeys();
        List<Text> mapperOutputValues = getMapperOutputValues();

        // initialize mocks
        // initialize subject
        AdmmIterationMapper subject = createSubject();

        // invoke target
        // assert
        assertMaps(subject, mapperInputKey, mapperInputValues, mapperOutputKeys, mapperOutputValues);

        // verify
    }

    private AdmmIterationMapper createSubject() {
        JobConf jobConf = new JobConf();
        jobConf.setInt("iteration.number", ITERATION_NUMBER);
        jobConf.set("columns.to.exclude", "");
        jobConf.setBoolean("add.intercept", true);
        jobConf.setFloat("regularization.factor", EXPECTED_LAMBDA_VALUE);
        jobConf.set("previous.intermediate.output.location", PREVIOUS_INTERMEDIATE_OUTPUT_LOCATION);
        jobConf.setFloat("rho", EXPECTED_RHO);

        AdmmIterationMapper subject = new AdmmIterationMapper();
        subject.configure(jobConf);
        return subject;
    }

    private List<IntWritable> getMapperOutputKeys() {
        List<IntWritable> mapperOutputKeys = new ArrayList<IntWritable>();
        mapperOutputKeys.add(ZERO);
        return mapperOutputKeys;
    }

    private List<Text> getMapperOutputValues() throws IOException {
        List<Text> mapperOutputValues = new ArrayList<Text>();
        AdmmReducerContext context0 = new AdmmReducerContext(
                EXPECTED_U_INITIAL,
                EXPECTED_X_INITIAL,
                EXPECTED_X_UPDATED,
                EXPECTED_Z_INITIAL,
                EXPECTED_PRIMAL_OBJECTIVE_VALUE,
                EXPECTED_RHO,
                EXPECTED_LAMBDA_VALUE);
        mapperOutputValues.add(new Text(SPLIT_ID + KEY_VALUE_DELMITER +
                admmReducerContextToJson(context0)));
        return mapperOutputValues;
    }

}
