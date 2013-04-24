package com.intentmedia.bfgs;

public class ScaleFactorsContext {

    private double[] sumOfSquares;
    private double[] sum;
    private int count;

    public ScaleFactorsContext(double[] sumOfSquares, double[] sum, int count) {
        updateScaleFactorsContext(sumOfSquares, sum, count);
    }

    public ScaleFactorsContext() {

    }

    public void updateScaleFactorsContext(double[] sumOfSquares, double[] sum, int count) {
        this.sumOfSquares = sumOfSquares;
        this.sum = sum;
        this.count = count;
    }

    public double[] getSumOfSquares() {
        return this.sumOfSquares;
    }

    public double[] getSum() {
        return this.sum;
    }

    public int getCount() {
        return this.count;
    }
}
