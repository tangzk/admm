package com.intentmedia.admm;

import org.junit.Test;

public class AdmmOptimizerDriverTest {

    @Test(expected = NullPointerException.class)
    public void testIgnoresExtraArguments() throws Exception {
        // initialize inputs
        String[] args = {"-outputPath", "outputPath",
                "-iterationsMaximum", "0",
                "-stepOutputBaseUrl", "abc",
                "-signalPath", "signalPath",
                "-regularizationFactor", "0.000001f"};
        // initialize mocks
        // initialize subject
        AdmmOptimizerDriver subject = new AdmmOptimizerDriver();

        // invoke target
        subject.run(args);

        // assert
        // verify
    }

}
