package com.intentmedia.admm;

import com.intentmedia.admm.helpers.BFGSTestHelper;
import com.intentmedia.bfgs.optimize.LogisticL2DifferentiableFunction;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class LogisticL2DifferentiableFunctionTest {
    private static final double RHO = 0.001;
    private static final double RESULT_AT_ZERO = 0.6931471805599322;
    private static final double RESULT_AT_NONZERO = 0.6917561514793987;

    @Test
    public void testEvaluationAtZero() throws Exception {
        double[][] features = BFGSTestHelper.featureMatrix();
        double[] labels = BFGSTestHelper.labelsVector();
        double[] u = new double[features[0].length];
        double[] z = new double[features[0].length];
        double[] x = new double[features[0].length];

        LogisticL2DifferentiableFunction myFunction = new LogisticL2DifferentiableFunction(features, labels, RHO, u, z);
        double result = myFunction.evaluate(x);
        assertEquals(RESULT_AT_ZERO, result, BFGSTestHelper.DELTA);
    }

    @Test
    public void testEvaluationAtNonzero() throws Exception {
        double[][] features = BFGSTestHelper.featureMatrix();
        double[] labels = BFGSTestHelper.labelsVector();
        double[] u = new double[features[0].length];
        double[] z = new double[features[0].length];
        double[] x = BFGSTestHelper.RESULT;

        LogisticL2DifferentiableFunction myFunction = new LogisticL2DifferentiableFunction(features, labels, RHO, u, z);
        double result = myFunction.evaluate(x);
        assertEquals(RESULT_AT_NONZERO, result, BFGSTestHelper.DELTA);
    }
}
