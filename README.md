## ADMM

ADMM is a Hadoop Map Reduce implementation of the Alternating Direction Method
of Multipliers (ADMM) algorithm [1].

ADMM is a generic optimization algorithm, this implementation includes a
logistic regression objective function, but can be extended to use other
objective functions.  To checkout and run an example in MapReduce standalone
mode:

```bash
git clone git@github.com:intentmedia/admm.git
cd admm
gradle jar
hadoop jar build/libs/admm.jar com.intentmedia.admm.AdmmOptimizerDriver \
-inputPath src/test/java/com/intentmedia/admm/files/logreg_features \
-outputPath output
```

[1] http://www.stanford.edu/~boyd/papers/admm_distr_stats.html

### Dependencies

* gradle

### Running Tests
```bash
gradle check
```
