## ADMM

![build](https://secure.travis-ci.org/intentmedia/admm.png?branch=master)

ADMM is a Hadoop Map Reduce implementation of the Alternating Direction Method
of Multipliers (ADMM) algorithm [[1]](http://www.stanford.edu/~boyd/papers/admm_distr_stats.html).

Details covering the implementation and statistical background are available in our
[IEEE BigData 2013 paper](http://intentmedia.github.io/assets/2013-10-09-presenting-at-ieee-big-data/pld_js_ieee_bigdata_2013_admm.pdf)
and our [presentation](http://intentmedia.github.io/2013/10/09/presenting-at-ieee-big-data/).

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
