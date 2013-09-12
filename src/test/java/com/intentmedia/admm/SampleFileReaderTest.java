package com.intentmedia.admm;

import org.junit.Test;

import static com.intentmedia.admm.helpers.BFGSTestHelper.featureMatrix;
import static com.intentmedia.admm.helpers.BFGSTestHelper.labelsVector;
import static org.junit.Assert.assertEquals;

public class SampleFileReaderTest {

    private static final int LINES_IN_FILE = 1000;
    private static final int NUMBER_OF_FEATURES = 5;

    @Test
    public void testSampleFileReaderLabels() throws Exception {
        double[] labels = labelsVector();
        assertEquals(LINES_IN_FILE, labels.length);
    }

    @Test
    public void testSampleFileReaderFeatures() throws Exception {
        double[][] features = featureMatrix();
        assertEquals(LINES_IN_FILE, features.length);
        assertEquals(NUMBER_OF_FEATURES, features[0].length);
    }
}
