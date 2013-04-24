/* Copyright (C) 2007 Vladimir Roubtsov. All rights reserved.
 */
package com.intentmedia.bfgs.optimize;

// ----------------------------------------------------------------------------
/**
 * 
 * @author Vlad Roubtsov, 2007
 */
abstract class AbstractOptimizer
{
    // public: ................................................................

    // protected: .............................................................
    
    protected AbstractOptimizer (final OptimizerParameters parameters)
    {
        final OptimizerParameters combined  = defaultParameters ().clone ();
        combined.combine (parameters);
        
        m_parameters = combined;
    }
    
    protected abstract OptimizerParameters defaultParameters ();
    
    protected final OptimizerParameters m_parameters;

    // package: ...............................................................

    // private: ...............................................................

}
// ----------------------------------------------------------------------------
