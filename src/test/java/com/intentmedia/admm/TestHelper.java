package com.intentmedia.admm;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mrunit.MapDriver;
import org.apache.hadoop.mrunit.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;

import java.io.IOException;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class TestHelper {
    public static <IK extends WritableComparable<?>, OK extends WritableComparable<?>> void assertMaps(
            Mapper<IK, Text, OK, Text> subject,
            IK inputKey,
            List<Text> inputValues,
            List<OK> outputKeys,
            List<Text> outputValues) {

        MapDriver<IK, Text, OK, Text> mapDriver = new MapDriver<IK, Text, OK, Text>(subject);

        for (Text inputValue : inputValues) {
            mapDriver.withInput(inputKey, inputValue);
        }
        for (int i = 0; i < outputValues.size(); i++) {
            mapDriver.withOutput(outputKeys.get(i), outputValues.get(i));
        }

        mapDriver.runTest();
    }

    public static <IK extends WritableComparable<?>, OK extends WritableComparable<?>> Text reduceToOneValue(
            Reducer<IK, Text, OK, Text> subject,
            IK inputKey,
            List<Text> inputValues) throws IOException {

        ReduceDriver<IK, Text, OK, Text> reduceDriver =
                new ReduceDriver<IK, Text, OK, Text>(subject)
                        .withInput(inputKey, inputValues);

        List<Pair<OK, Text>> output = reduceDriver.run();
        assertEquals(1, output.size());

        return output.get(0).getSecond();
    }
}