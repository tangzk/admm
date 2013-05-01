package com.intentmedia.admm;

import org.junit.Test;
import org.kohsuke.args4j.CmdLineParser;

import java.net.URI;

import static org.junit.Assert.assertEquals;

public class AdmmOptimizerDriverArgumentsTest {

    private static final URI OUTPUT_PATH = URI.create("s3n://my-bucket/output/XXX/");
    private static final String SIGNAL_PATH = "inputPath";

    @Test
    public void testSignalPath() throws Exception {
        // initialize inputs
        // initialize mocks
        // initialize subject
        AdmmOptimizerDriverArguments subject = new AdmmOptimizerDriverArguments();

        // invoke target
        new CmdLineParser(subject).parseArgument(
                "-outputPath",
                OUTPUT_PATH.toString(),
                "-inputPath",
                SIGNAL_PATH);

        // assert
        assertEquals(SIGNAL_PATH, subject.getInputPath());

        // verify
    }

    @Test
    public void testIterationsMaximum() throws Exception {
        // initialize inputs
        Integer iterationsMaximum = 1;

        // initialize mocks
        // initialize subject
        AdmmOptimizerDriverArguments subject = new AdmmOptimizerDriverArguments();

        // invoke target
        new CmdLineParser(subject).parseArgument(
                "-outputPath",
                OUTPUT_PATH.toString(),
                "-inputPath",
                SIGNAL_PATH,
                "-iterationsMaximum",
                iterationsMaximum.toString());

        // assert
        assertEquals((int) iterationsMaximum, subject.getIterationsMaximum());

        // verify
    }

    @Test
    public void testRegularizationFactor() throws Exception {
        // initialize inputs
        Double regularizationFactor = 0.5;

        // initialize mocks
        // initialize subject
        AdmmOptimizerDriverArguments subject = new AdmmOptimizerDriverArguments();

        // invoke target
        new CmdLineParser(subject).parseArgument(
                "-outputPath",
                OUTPUT_PATH.toString(),
                "-inputPath",
                SIGNAL_PATH,
                "-regularizationFactor",
                regularizationFactor.toString());

        // assert
        assertEquals((double) regularizationFactor, subject.getRegularizationFactor(), 0.0);

        // verify
    }

    @Test
    public void testAddIntercept() throws Exception {
        // initialize inputs
        boolean addIntercept = true;

        // initialize mocks
        // initialize subject
        AdmmOptimizerDriverArguments subject = new AdmmOptimizerDriverArguments();

        // invoke target
        new CmdLineParser(subject).parseArgument(
                "-outputPath",
                OUTPUT_PATH.toString(),
                "-inputPath",
                SIGNAL_PATH,
                "-addIntercept");

        // assert
        assertEquals(addIntercept, subject.getAddIntercept());

        // verify
    }

    @Test
    public void testRegularizeIntercept() throws Exception {
        // initialize inputs
        boolean regularizeIntercept = true;

        // initialize mocks
        // initialize subject
        AdmmOptimizerDriverArguments subject = new AdmmOptimizerDriverArguments();

        // invoke target
        new CmdLineParser(subject).parseArgument(
                "-outputPath",
                OUTPUT_PATH.toString(),
                "-inputPath",
                SIGNAL_PATH,
                "-regularizeIntercept");

        // assert
        assertEquals(regularizeIntercept, subject.getRegularizeIntercept());

        // verify
    }

    @Test
    public void testColumnsToExclude() throws Exception {
        // initialize inputs
        String columnsToExclude = "columnsToExclude";

        // initialize mocks
        // initialize subject
        AdmmOptimizerDriverArguments subject = new AdmmOptimizerDriverArguments();

        // invoke target
        new CmdLineParser(subject).parseArgument(
                "-outputPath",
                OUTPUT_PATH.toString(),
                "-inputPath",
                SIGNAL_PATH,
                "-columnsToExclude",
                columnsToExclude);

        // assert
        assertEquals(columnsToExclude, subject.getColumnsToExclude());

        // verify
    }
}
