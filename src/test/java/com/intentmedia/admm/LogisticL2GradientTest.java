package com.intentmedia.admm;

import org.junit.Test;
import static org.junit.Assert.assertEquals;

public class LogisticL2GradientTest {
    private static final double RHO = 0.001;
    private static final double[] RESULT_AT_ZERO = {-68.0, -36.06265569000002, -64.76277607999997, -44.59906977000005, -32.070343389999984};
    private static final double[] RESULT_AT_NONZERO = {-119.57330796148507, -60.37123213593641, -75.54533805026209, -65.35442766512425, -58.937017281823};

    @Test
    public void testEvaluationAtZero() throws Exception {
        double[][] features = ADMMTestHelper.featureMatrix();
        double[] labels = ADMMTestHelper.labelsVector();
        double[] u = new double[features[0].length];
        double[] z = new double[features[0].length];
        double[] x = new double[features[0].length];
        double[] out = new double[features[0].length];

        LogisticL2Gradient myGradient = new LogisticL2Gradient(features, labels, RHO, u, z);
        myGradient.evaluate(x, out);
        for(int i = 0; i < out.length; i++) {
            assertEquals(RESULT_AT_ZERO[i], out[i], ADMMTestHelper.DELTA);
        }
    }

    @Test
    public void testEvaluationAtNonzero() throws Exception {
        double[][] features = ADMMTestHelper.featureMatrix();
        double[] labels = ADMMTestHelper.labelsVector();
        double[] u = new double[features[0].length];
        double[] z = new double[features[0].length];
        double[] x = ADMMTestHelper.RESULT;
        double[] out = new double[features[0].length];

        LogisticL2Gradient myGradient = new LogisticL2Gradient(features, labels, RHO, u, z);
        myGradient.evaluate(x, out);
        for(int i = 0; i < out.length; i++) {
            assertEquals(RESULT_AT_NONZERO[i], out[i], ADMMTestHelper.DELTA);
        }
    }
}
