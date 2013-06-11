/* Copyright (C) 2005 Vladimir Roubtsov. All rights reserved.
 */
package com.intentmedia.bfgs.optimize;

/**
 * @author Vlad Roubtsov
 */
public interface IFunctionGradient {
    void evaluate(double[] x, double[] out);

}