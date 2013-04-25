package com.intentmedia.admm;

public final class AdmmHelper {

    public static final double TARGET_STANDARD_DEVIATION = 0.5;
    private static final double MAXIMUM_SCALING_FACTOR = 1.0;

    private AdmmHelper() {
    }

    public static double normalize(double value, double variance) {
        return value * calculateScalingFactor(variance);
    }

    public static double calculateScalingFactor(double variance) {
        double stdDev = Math.sqrt(variance);
        return (stdDev > 0) ? Math.min(TARGET_STANDARD_DEVIATION / stdDev, MAXIMUM_SCALING_FACTOR) : 1;
    }
}
