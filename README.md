## ADMM

![build](https://secure.travis-ci.org/intentmedia/admm.png?branch=master)

ADMM is a Hadoop Map Reduce implementation of the Alternating Direction Method
of Multipliers (ADMM) algorithm [[1]](http://www.stanford.edu/~boyd/papers/admm_distr_stats.html).

ADMM is a generic optimization algorithm.  This implementation includes a
logistic regression objective function, however it can be extended to use other
objective functions.

To checkout the code and run an example in MapReduce standalone
mode, execute:

```bash
git clone git@github.com:intentmedia/admm.git
cd admm
gradle jar
hadoop jar build/libs/admm.jar com.intentmedia.admm.AdmmOptimizerDriver \
-inputPath src/test/java/com/intentmedia/admm/files/logreg_features \
-outputPath output
```

### Dependencies

* gradle

### Running Tests
```bash
gradle check
```
