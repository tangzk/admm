package com.intentmedia.admm;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class LogisticL2DifferentiableFunctionTest {
    private static final double RHO = 0.001;
    private static final double RESULT_AT_ZERO = 693.1471805599322;
    private static final double RESULT_AT_NONZERO = 693.8702452444736;

    @Test
    public void testEvaluationAtZero() throws Exception {
        double[][] features = ADMMTestHelper.featureMatrix();
        double[] labels = ADMMTestHelper.labelsVector();
        double[] u = new double[features[0].length];
        double[] z = new double[features[0].length];
        double[] x = new double[features[0].length];

        LogisticL2DifferentiableFunction myFunction = new LogisticL2DifferentiableFunction(features, labels, RHO, u, z);
        double result = myFunction.evaluate(x);
        assertEquals(RESULT_AT_ZERO, result, ADMMTestHelper.DELTA);
    }

    @Test
    public void testEvaluationAtNonzero() throws Exception {
        double[][] features = ADMMTestHelper.featureMatrix();
        double[] labels = ADMMTestHelper.labelsVector();
        double[] u = new double[features[0].length];
        double[] z = new double[features[0].length];
        double[] x = ADMMTestHelper.RESULT;

        LogisticL2DifferentiableFunction myFunction = new LogisticL2DifferentiableFunction(features, labels, RHO, u, z);
        double result = myFunction.evaluate(x);
        assertEquals(RESULT_AT_NONZERO, result, ADMMTestHelper.DELTA);
    }
}
