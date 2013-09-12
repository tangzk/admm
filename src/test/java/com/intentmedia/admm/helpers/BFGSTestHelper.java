package com.intentmedia.admm.helpers;

import com.intentmedia.admm.SampleFileReader;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.io.Text;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public final class BFGSTestHelper {

    public static final double[] RESULT = {
            -0.6584792103604006,
            0.05862803577268928,
            0.7306727562934296,
            0.260894417710139,
            -0.11847956322896916};
    public static final double DELTA = 0;
    public static final String BASE_PATH = "src/test/java/com/intentmedia/admm/files/";
    private static final String LABELS_FILE = BASE_PATH + "logreg_labels";
    private static final String FEATURES_FILE = BASE_PATH + "logreg_features";
    private static final String SAMPLE_DATA_FILE = BASE_PATH + "logreg_training";

    private BFGSTestHelper() {
    }

    public static double[][] featureMatrix() {
        SampleFileReader sampleFileReader = new SampleFileReader();
        return sampleFileReader.readFeatures(FEATURES_FILE);
    }

    public static double[] labelsVector() {
        SampleFileReader sampleFileReader = new SampleFileReader();
        return sampleFileReader.readLabels(LABELS_FILE);
    }

    public static List<Text> getMapperInputValues() throws IOException {
        List<Text> mapperInputValues = new ArrayList<Text>();
        File sampleAdmmDataFile = new File(SAMPLE_DATA_FILE);
        String sampleAdmmData = FileUtils.readFileToString(sampleAdmmDataFile);
        mapperInputValues.add(new Text(sampleAdmmData));
        return mapperInputValues;
    }
}
