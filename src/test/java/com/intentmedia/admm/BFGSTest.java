package com.intentmedia.admm;

import com.intentmedia.admm.LogisticL2DifferentiableFunction;
import com.intentmedia.bfgs.optimize.BFGS;
import com.intentmedia.bfgs.optimize.IOptimizer;
import com.intentmedia.bfgs.optimize.OptimizerParameters;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class BFGSTest {

    private static final double RHO = 0.001;

    @Test
    public void bfgsTest() {
        OptimizerParameters optimizerParameters = new OptimizerParameters();
        BFGS<LogisticL2DifferentiableFunction> bfgs = new BFGS<LogisticL2DifferentiableFunction>(optimizerParameters);

        double[][] features = ADMMTestHelper.featureMatrix();
        double[] labels = ADMMTestHelper.labelsVector();
        double[] u = new double[features[0].length];
        double[] z = new double[features[0].length];
        double[] xStart = new double[features[0].length];

        normalizeAValues(features);

        LogisticL2DifferentiableFunction myFunction = new LogisticL2DifferentiableFunction(features, labels, RHO, u, z);
        IOptimizer.Ctx optimizationContext = new IOptimizer.Ctx(xStart);
        bfgs.minimize(myFunction, optimizationContext);

        for (int i = 0; i < xStart.length; i++) {
            assertEquals(ADMMTestHelper.RESULT[i], optimizationContext.m_optimumX[i], ADMMTestHelper.DELTA);
        }
    }

    public void normalizeAValues(double[][] a) {
        for (int col = 0; col < a[0].length; col++) {
            boolean isBinary = true;

            for (double[] row : a) {
                if (row[col] != 0 && (row[col] - 1) != 0) {
                    isBinary = false;
                }
            }

            if (!isBinary) {
                double average = 0;

                for (double[] row : a) {
                    average += row[col];
                }
                average /= a.length;
                double squaredSum = 0;
                for (double[] row : a) {
                    squaredSum += Math.pow(row[col] - average, 2);
                }
                double stdDev = squaredSum / (a.length - 1);
                for (int row = 0; row < a.length; row++) {
                    a[row][col] /= Math.max(stdDev, 0.25) * 2;
                }
            }
        }
    }
}
