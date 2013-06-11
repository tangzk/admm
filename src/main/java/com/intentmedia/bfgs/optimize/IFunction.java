/* Copyright (C) 2005 Vladimir Roubtsov. All rights reserved.
 */
package com.intentmedia.bfgs.optimize;

/**
 * @author Vlad Roubtsov
 */
public interface IFunction {
    double evaluate(double[] x);

}