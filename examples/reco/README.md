# Example Collaborative filtering algorithms

This examples implements various simple collaborative filtering algorithms. The purpose of the example is to show how to use Occult to implement algorithms.

## Install

```
# -u will update existing package, -x shows what's going on.
go get -u -x  github.com/akualab/occult/examples/reco
```

Install [leveldb](https://code.google.com/p/leveldb/) which is used to store the data set. I tested in MacOSX: `brew install leveldb`

Finally, run the example:

```
cd GOPATH/src/github.com/akualab/occult/examples/reco/
go run *.go
```

## Walkthrough

### reco.go

This file has the entry point for the program `manin()` and calls the top level functions:
* Download data set (if first time).
* Build the leveldb databases (for train and test).
* Train model.
* Evaluate predictions on a disjoint data set.

### train.go

This file shows you how to use occult to implement the trainer.
* Reads data from database as needed.
* Comuptes various aggregations.
* Trains a simple matrix factorization algorithms using gradient descent.
* Returns the model in data struct CF.

### eval.go

Runs the evaluation using the CF data struct trained by trainer and prints results.

### cf.go

Defines various data structures and methods that are used to train and evaluate results.

### data.go

Low level details to download data, build DB, and query values.


## Compare concurrent computation.

The Processor method MapAll() executes the process using runtime.NumCPU() workers.

Task: ratings distribution

```
Host: MacBook Air

Concurrent   Num Procs     Num Workers    Time
No           2             NA             1.85 s
Yes          2             2              1.20 s (35% speedup)
```

## References

* Algorithms articles:
   * http://www.stanford.edu/~lmackey/papers/cf_slides-pml09.pdf
   * https://datajobs.com/data-science-repo/Recommender-Systems-[Netflix].pdf
   * http://sifter.org/~simon/journal/20061211.html
   * http://www.slideshare.net/DKALab/collaborativefilteringfactorization
   * http://www.quuxlabs.com/blog/2010/09/matrix-factorization-a-simple-tutorial-and-implementation-in-python/
* Data: http://grouplens.org/datasets/movielens/
* Published results: http://mymedialite.net/examples/datasets.html
