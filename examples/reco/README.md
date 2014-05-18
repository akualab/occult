# Example: Predict Movie Ratings using Collaborative Filtering

This examples implements various simple collaborative filtering algorithms. The purpose of the example is to show how to use Occult to implement machine learning algorithms.

## Install

Install [leveldb](https://code.google.com/p/leveldb/) which is used to store the data set. I tested in MacOSX: `brew install leveldb`

```
# -u will update existing package, -x shows what's going on.
go get -u -x  github.com/akualab/occult/examples/reco
```

Finally, run the example on a single node:

```
cd GOPATH/src/github.com/akualab/occult/examples/reco/
go run *.go
```

Now, let's try to run on two nodes. Open two terminals on the same machine and type in sequence:

```
# Starts a server with id node 1.
reco -config=reco-cluster.yaml -node=1 -server -v=2 -logtostderr

# Starts a client with id node 0.
reco -config=reco-cluster.yaml -node=0 -v=2 -logtostderr
```

Each node will start a server. Once the server is up, each node tries to establish a connection to the remote nodes. Finally, when all remote nodes are connected, the client (node=0) will start running the processors and assigning work to the nodes.

The config file `reco-config.yaml` provides the network address of the nodes. (Later we will add a cluster coordination layer to manage the nodes.)

```yaml
app:
  name: "cf"
  cache_cap: 1000
cluster:
  name: "local"
  nodes:
    - id: 0
      addr: ":7000"
    - id: 1
      addr: ":7001"
```

To get a more verbose log, set `v` to a higher number. The option `-v=5` will dump a lot of data. See [glog](http://godoc.org/github.com/golang/glog) for more logging command options.

The server node (node=1) waits for work requests from the client. The client (node=0) coordinates the work according to the router algorithm. For now, we use a trivial round-robin router that assigns block of keys to each node. Note that the client is also doing work in this case. Should be possible to set up nodes to be servers or clients or both. Even multiple clients running the same program should work.

To remove the data set, run: `rm -rf out`.

## Walkthrough

### reco.go

This file has the entry point for the program `main()` and calls the top level functions:
* Download data set (if first time).
* Build the leveldb databases (for train and test).
* Train model.
* Evaluate predictions on a disjoint data set.

### train.go

This file shows you how to use occult to implement the trainer.
* Reads data from database as needed.
* Computes various aggregations.
* Trains a simple matrix factorization algorithms using gradient descent.
* Returns the model in data struct CF.

NOTE: the processor output is of type occult.Value which is an interface. To communicate across
nodes, we send the values using GOB encoding. For GOB to work with interfaces, we need to register
these custom types. (There is no error when the type is not registered. Something that can result
in many hours of debugging!)

### eval.go

Runs the evaluation using the CF data struct trained by trainer and prints results.

### cf.go

Defines various data structures and methods that are used to train and evaluate results.

### data.go

Low level details to download data, build DB, and query values.


## Compare concurrent computation (single node).

The Processor method MapAll() executes the process using runtime.NumCPU() workers.

Task: ratings distribution

```
Host: MacBook Air

Concurrent   Num Procs     Num Workers    Time
No           2             NA             1.85 s
Yes          2             2              1.20 s (35% speedup)
```

Test train times using LRU cache. As you can see, without the cache, the processing would be about two orders of magnitude slower. (Not a fair comparison, though, global values are computed in a first pass reading all the data, with no cache the program needs to re-compute global values every time they are needed.) The advantage, however, is that adding more cache saves you from having to optimize manually.

```
Notes:
- leveldb cache disabled to simulate slow data access.
- cache size is based on num elements (not memory usage)

Chunk Size   Cache Cap     Time
200          c <= 300      ~5 minutes
200          c >= 400      ~7 seconds
100          400           ~5 minutes
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
