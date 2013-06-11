/* Copyright (C) 2005 Vladimir Roubtsov. All rights reserved.
 */
package com.intentmedia.bfgs.optimize;

/**
 * @author Vlad Roubtsov, (C) 2005
 */
public interface IDifferentiableFunction extends IFunction {
    IFunctionGradient gradient();

}