# Project Occult

**Occult is an open-source, distributed, cache-oriented, array processing architecture for scientific computing.**

Quote from [Wikipedia](http://en.wikipedia.org/wiki/Occult): *From the scientific perspective, occultism is regarded as unscientific as it does not make use of the standard scientific method to obtain facts.*

That's what it feels like to do scientific computing on distributed systems!

## Design Goals

* Simplicity of implementation, deployment, and use.
* A programming model that hides the complexity of distributed systems.
* Minimize network traffic and read operations from slow data sources.
* Cache data in memory to speed up processing. Dynamically route work to where the data is.
* Avoid centralized management to achieve unlimited scalability and elasticity.
* Unified platform for batch-, iterative-, and stream-processing.
* Fault-tolerant.

This is an experimental package to explore distributed array processing architectures using the Go
programming language.

The inspiration came from various sources including ideas behind [SciDB](http://scidb.org/), [Apache Spark](http://spark.apache.org/), S4 ([PDF](http://www.stanford.edu/class/cs347/reading/S4PaperV2.pdf)), and many other open source projects.

## Use Cases

* Process time series data.
* Detect anomalies in a data center using streams of measurements.
* Analyze sensor data in Internet of Things (IoT) applications.
* Detect intruders in a network.
* Set alarms in a surveillance system that analyzes video using computer vision algorithms.
* Train a model to predict clicks on a web page.

## How It Works

* Data is ingested by a distributed store. (Not part of the project.)
* A data source is organized as a sequence of records with an integer, 64-bit key.
* An app is an executable program deployed to all the nodes in the cluster.
* An app is a graph of simple processing functions where a processor depends on the outputs of other processors.
* Processors return a Value for a given key. (Pull architecture.)
* The application graph may depend on slow data sources.
* Processors cache intermediate results transparently by key.

## Example

source: [occult_test.go](https://github.com/akualab/occult/blob/master/occult_test.go)

To run occult_test:

```
# If you need to install Go, see:
# install: http://golang.org/doc/install
# setup: http://golang.org/doc/code.html
go get -u github.com/akualab/occult
cd GOPATH/src/github.com/akualab/occult
go test -v
```

In this example,
* `randomFunc` is the data source which provides an array of ints.
* `windowFunc` is applied every N samples, returns a slice of ints of length winSize.
* `sortFunc` sorts the slice of ints returned by the window.
* `quantileFunc` returns a slice with the values of the quantiles (eg. n=2 returns one value, the median.)

The application is a collection of short processing functions. The processing details are hidden. To get a value, for example `quantiles` for key 33, simply run:

`
result, err := quantile(33)
`

See also the collaborative filtering example: [README.md](https://github.com/akualab/occult/blob/master/examples/reco/README.md)

## Under the Hood

The core functionality is in [occult.go](https://github.com/akualab/occult/blob/master/occult.go).

The approach is to provide a basic type called ProcFunc:

```go
type ProcFunc func(key uint64, ctx *Context) (Value, error)
```

with only two arguments, the key and a context object. The wiring of the processors is handled by `Context` as follows:

```go
aProcessor := ctx.Inputs()[0] // get the first input which points to aProcesror.
anotherProcessor := ctx.Inputs()[1] // get the second input and as many as required.
```

to get a value from the input:

```go
var uint64 key = 111 // some key
in0, err := aProcessor(key) // Processors return errors.
if err == occult.ErrEndOfArray {
	break // when we don't know the length of the array, we rely on errors.
}
s := in0.(MyType) // use type assertion to uncover the underlying type
```

the variable `s` has the value produced by `aProcessor` for `key` 111.

Finally, to build an application and get Processor instances, we add the ProcFunc functions to an app using `app.Add()` and `app.AddSource()`. The latter will set a flag to indicate that is a slow source. This information will be used to allocate work to nodes efficiently.

Note that a ProcFunc can be used to create more than one processor. The Processor instances will have the same functionality but may use different inputs and parameters. ProcFunc can be written to be highly reusable or highly customized for the application (one-time use).

As always, with Go, we decide to reuse or rewrite using a pragmatic approach. Writing custom code can be much faster and cleaner than writing reusable code. Fewer levels of indirection makes code simpler and easier to understand.

## Using a Cluster

We implemented initial cluster functionality for experimentation. Any node can do any work but the router is responsible to make the distribution of work efficient. For now router is doing a dumb round-robin.To send values across the wire, we use the [RPC](http://golang.org/pkg/net/rpc/) package. Values are encoding using GOB. Custom types must be registered.

### Finding Memory

Performance is achieved by distributing work among the nodes in the cluster. However, any node can do any work. A parallel system will be responsible for maintaining *routing tables* that instruct the app where to get the work done for a given index. This information is built dynamically. For example, to get `someWork(333)`, the app will look up node for the (processor, key) pair. If the info does not exist, the node is chosen based on load or other criteria. However, the mapping between work and node is broadcasted to all the nodes in the cluster to update all the local routing tables.

Each processor instance has a separate LRU cache. Values are cached by key. The code was adapted from the [vitess](https://code.google.com/p/vitess/source/browse/go/cache/lru_cache.go). For now, all cached have the same capacity (max number of items). However, cache capacity can be managed dynamically, based on performance.

### Messaging

Because all nodes can do any work, the system feels like a stateless machine, even though state is encoded in the processor graph as a derivative of the original data sources. In other words, messages can get lost and nodes can be added or removed from the cluster without causing failures, only temporary degradation in performance. The only requirement is to have the original data sources available.

## Next Steps

* Get feedback on overall architecture and API.
* Decide: does the world need this system? Written in Go?
* Improve cluster functionality. (routing, cluster coordination, failure mgmt, config changes.)
* Find a task (sponsor?) to build an app for testing using a very large data set. (suggestions?)

Thanks! Leo Neumeyer, May 12, 2014.
* leo@akualab.com
* @leoneu
* https://www.linkedin.com/in/leoneu
