# Cache-Oriented Array Processing (COAP)

`Copyright (c) 2013 AKUALAB INC., All rights reserved.`

This is an experimental package to explore distributed aray processing architectures using the Go
programming language.

The inspiration came from various sources including the ideas behind SciDB, a distributed database
designed for scientific computation using large data sets. I also wanted to improve the ideas that led to the design of S4, a distributed stream processing platofrm that we developed at Yahoo! This is still work in progress, suggestion are welcomed!

The typical use case is to process time series data. For example, in a data center,
metrics can be processed to detect failures, degradation in performance, or any anomalies.
A surveilance application may store images that need to be analyzed using computer vision
algorithms. However, any application that involves lareg data sets can potentially be
implemented using COAP.

## Design Goals

* Simplicity of implementation, deployment, and use.
* A programming model that hides the complexity of distributed systems.
* Minimize network traffic and reads from slow data sources.
* Cache data in memory to speed up processing. This applies to iterative algorithms and to cases where multiple applications access the same data concurrently.
* Avoid centralized management to achieve unlimited scalability.
* A single architecture for bacth and stream processing.

## How It Works

* Data is ingested by a distributed store. (Pushed into data store.)
* A data source is organized as a temporal sequence of records with an integer key.
* An app is an executable program deployed to all the nodes in a cluster.
* An app has a network of interconnected processors.
* Processor provide values for key on demand. (Value is pulled from processor.)
* A processor gets input data from other processors.
* The leaf of the tree are data sources (eg. the distributed key-value store.)
* Processors cache intermediate results indexed by the key.

## Example

(see coap_test.go)

To run the example:

`
# If you need to install Go, see:
# install: http://golang.org/doc/install
# setup: http://golang.org/doc/code.html
git clone https://github.com/akualab/coap
cd coap
go test -v
`

* Source randomFunc provides an array of ints
* windowFunc is applied every N samples, returns a slice of ints of length winSize.
* sortFunc Sort sorts the slice of ints returned by the window.
* quantileFunc returns a slice with the values of the quantiles (eg. n=2 returns one value, the median.)

Programming and wiring the functions is very easy. All the details are pretty much hidden. To get the quantiles for a index 33, simply run:

`
result, err := quantile(33)
`

## Under the Hood

The initial prototype is implemented in coap.go and only runs on a single host. The goal is to discuss the design before getting to deep into implementation details.

The implementation is only a few lines of code but appears to satisfy many of the requirements. Using the Go programming style, I chose to implement processors using functions. A processor is implemented usign the ProcFunc type. To add a processor to an app and wire its inputs, we use the methods:

* app.Add()
* app.AddSkip()
* app.AddSource()

These methods create the processor instance with specific parameters and inputs. AddSkip specifies how to sample the inputs. This may seem strange but is there to help determine the index of the source values which is needed to create affinity with a cluster node. AddSource hints the app that the processor instance is a persistent store whose access is slow.

Perhaps the most important aspect of the design is that affinity is not required. It's only there to optimize performance. This feature makes it incredibly easy to add and remove nodes and to allocate tasks in the cluster. For example, a node failure results in degraded performance because active requests will time out and some new requests will hit the persistent store. Performance will normalize after the caches are filled up again. When a node is overloaded due to skewed requests, a load balancing algorithms can be used to use a different node.

## Next Steps

As you see this is a big idea with a very simple implementation. Let me know what you thin, does this makes sense? What use cases shoudl we target?

Thanks! Leo Neumeyer, April 2014.
* leo@akualab.com
* @leoneu
* https://www.linkedin.com/in/leoneu
