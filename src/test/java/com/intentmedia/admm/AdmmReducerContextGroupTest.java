package com.intentmedia.admm;

import org.apache.hadoop.io.Text;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Logger;
import java.util.regex.Pattern;

import static com.intentmedia.admm.AdmmIterationHelper.admmReducerContextToJson;
import static org.junit.Assert.assertEquals;

public class AdmmReducerContextGroupTest {
    private static final Pattern KEY_VALUE_DELIMITER = Pattern.compile("::");
    private static final int NUMBER_OF_REDUCER_CONTEXTS = 3;
    private static final double[] STATIC_DOUBLE_ARRAY = {1.0, 2.0, 3.0, 4.0};
    private static final Logger LOG = Logger.getLogger(AdmmIterationMapper.class.getName());
    private static final Double DELTA = 1e-6;

    @Test
    public void testConstructor() throws Exception {
        // initialize inputs
        Iterator<Text> mapperResults = getMapperResults();

        // initialize mocks
        // initialize subject
        AdmmReducerContextGroup admmReducerContextGroup =
                new AdmmReducerContextGroup(mapperResults, NUMBER_OF_REDUCER_CONTEXTS, LOG, 0);

        // invoke target
        // assert
        assertEquals(admmReducerContextGroup.getSplitIds()[0], "0");
        assertEquals(admmReducerContextGroup.getSplitIds()[1], "1");
        assertEquals(admmReducerContextGroup.getSplitIds()[2], "2");

        assertEquals(admmReducerContextGroup.getXUpdated()[0][0], 1.0, DELTA);
        assertEquals(admmReducerContextGroup.getXUpdated()[2][1], 2.0, DELTA);

        // verify
    }

    private Iterator<Text> getMapperResults() throws IOException {
        List<Text> reducerContextList = new ArrayList<Text>();
        for (int i = 0; i < NUMBER_OF_REDUCER_CONTEXTS; i++) {
            AdmmReducerContext context = new AdmmReducerContext(
                    STATIC_DOUBLE_ARRAY, STATIC_DOUBLE_ARRAY, STATIC_DOUBLE_ARRAY,
                    STATIC_DOUBLE_ARRAY, 1.0, 1.0, 1.0);
            reducerContextList.add(new Text(Integer.toString(i) + KEY_VALUE_DELIMITER +
                    admmReducerContextToJson(context)));
        }

        return reducerContextList.iterator();
    }
}