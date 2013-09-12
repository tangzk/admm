package com.intentmedia.admm;

import com.intentmedia.admm.helpers.BFGSTestHelper;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class LogisticL2GradientTest {

    private static final double RHO = 0.001;
    private static final double[] RESULT_AT_ZERO =
            {-0.068, -0.03606265569000002, -0.06476277607999997, -0.04459906977000005, -0.032070343389999985};
    private static final double[] RESULT_AT_NONZERO =
            {-0.11402082362240264, -0.05742112609389968, -0.07198555880538735, -0.06218922010977889, -0.05608739612801881};

    @Test
    public void testEvaluationAtZero() throws Exception {
        double[][] features = BFGSTestHelper.featureMatrix();
        double[] labels = BFGSTestHelper.labelsVector();
        double[] u = new double[features[0].length];
        double[] z = new double[features[0].length];
        double[] x = new double[features[0].length];
        double[] out = new double[features[0].length];

        LogisticL2Gradient myGradient = new LogisticL2Gradient(features, labels, RHO, u, z);
        myGradient.evaluate(x, out);
        for (int i = 0; i < out.length; i++) {
            assertEquals(RESULT_AT_ZERO[i], out[i], BFGSTestHelper.DELTA);
        }
    }

    @Test
    public void testEvaluationAtNonzero() throws Exception {
        double[][] features = BFGSTestHelper.featureMatrix();
        double[] labels = BFGSTestHelper.labelsVector();
        double[] u = new double[features[0].length];
        double[] z = new double[features[0].length];
        double[] x = BFGSTestHelper.RESULT;
        double[] out = new double[features[0].length];

        LogisticL2Gradient myGradient = new LogisticL2Gradient(features, labels, RHO, u, z);
        myGradient.evaluate(x, out);
        for (int i = 0; i < out.length; i++) {
            assertEquals(RESULT_AT_NONZERO[i], out[i], BFGSTestHelper.DELTA);
        }
    }
}
