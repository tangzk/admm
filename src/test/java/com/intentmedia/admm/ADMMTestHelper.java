package com.intentmedia.admm;

public final class ADMMTestHelper {

    public static final double[] RESULT = {
            -0.7074638444654979,
            0.06934285431561207,
            0.7483554158831095,
            0.2739342447825968,
            -0.11343770210027582};
    public static final double DELTA = 0;
    private static final String BASE_PATH = "test/java/com/intentmedia/bfgs/files/";
    private static final String LABELS_FILE = BASE_PATH + "logreg_labels";
    private static final String FEATURES_FILE = BASE_PATH + "logreg_features";

    private ADMMTestHelper() {
    }

    public static double[][] featureMatrix() {
        SampleFileReader sampleFileReader = new SampleFileReader();
        return sampleFileReader.readFeatures(FEATURES_FILE);
    }

    public static double[] labelsVector() {
        SampleFileReader sampleFileReader = new SampleFileReader();
        return sampleFileReader.readLabels(LABELS_FILE);
    }
}

