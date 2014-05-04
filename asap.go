/*
Package asap implements an API for Array Storage and Processing in distributed systems.

The typical use case is to process time series data. For example, in a data center,
metrics can be processed to detect failures, degradation in performance, or any anomalies.
A surveilance application may store images that need to be analyzed using computer vision
algorithms.

The basic idea is to create the illusion that a local program can operate on a Petabyte-size
slice with no knowledge of how the actual computations is executed on a 1000-node cluster. The
appliaction developer can iterate through the virtual slice while the underlying cluster computes
the results.

Typically, there is a persistent, fault-tolerant store that contains the raw data.
Derivations of the raw data may be stored or cached in the cluster to improve performance.

Cluster nodes can easily be added or removed because all the data can be re-generated when required.

When processing large amounts of data, it is important to avoid losing the work in the
presence of failures. The system should be capable of detecting failures, re-assign resources, and
re-compute only the work that got lost.

A failed node can be removed from the cluster. The cluster managament system must detect
the change in topology and adapt the execution instrutions automatically.

Because the client runs the application on a single host, we need a strategy to recover the application process when the client fails. For example, by caching derived data, a new client could be restarted but will not need to re-compute all the work. Another strategy coudl be to run two clients concurrently where the primary client computes and caches and the secondary one follows. Because the requests from the secondary client occur a short time after the primary, we increase the probability of hitting the cache and do very little additional work. When the primary client fails, the secondary will continue running and autmatically will start computing from raw data. For greater redundancy, add additional client replicas.

An ASAP application does not require an external DBMS or cluster management system. Each application is a stand-alone, elastic, fault-toleant, distributed system completely isolated from other applications. This approach greatly reduces complexity at design and deployment times.

In ASAP, application, computaion, storage, and caching are all designed to work together in an
optimal manner. The paradigm of storing data first and compute later quickly degrades in a cluster
environment, especially when moving large amounts of data becomes prohibetly. In ASAP it is possible to balance storage and computational complexity according to the application needs. For example, frequently used data is stored and the rest is computed. This can be done transparently without adding complexity to the application.

Design components:

 * Application graph.
 * Cluster.
 * Remote communication layer.
 * Caching.

*/
package asap

import "errors"

var (
	ErrEndOfArray = errors.New("reached the end of the array")
)

// All processors must be implemented using this function type.
type ProcFunc func(key uint64, ctx *Context, in ...Processor) (Value, error)

// The context provides internal information for processor instances.
// The Options field can be used to pass parameters to processors.
type Context struct {
	Options  interface{}
	Skip     int
	cache    *cache
	procFunc ProcFunc
	proc     Processor
	inputs   []Processor
	isSource bool
}

// An App coordinates the execution of a set of processors.
type App struct {
	Name  string
	procs map[int]*Context
}

// Creates a new App.
func NewApp(name string) *App {

	app := &App{Name: name}
	app.procs = make(map[int]*Context)
	return app
}

// The value returned by Processors.
type Value interface{}

// A Processor instance.
// Once the processor instance is created, teh parameters and inputs cannot
// be changed.
type Processor func(key uint64) (Value, error)

// Same as Add but indicating that this is a presistent source.
// The system will attempt to use the same cluster node for a given key. This
// affinity will increase the cache hit rate and minimize reads from the persistent
// source.
func (app *App) AddSource(fn ProcFunc, opt interface{}, inputs ...Processor) Processor {

	ctx := app.createContext(fn, opt, inputs...)
	ctx.isSource = true
	ctx.proc = procInstance(ctx)
	return ctx.proc
}

// Same as Add but inputs are sub-sampled.
// For example if skip = 4 and key = 10, then the inputs are passed inKey = 4 * 10 = 40
func (app *App) AddSkip(skip int, fn ProcFunc, opt interface{}, inputs ...Processor) Processor {

	ctx := app.createContext(fn, opt, inputs...)
	ctx.Skip = skip
	ctx.proc = procInstance(ctx)
	return ctx.proc
}

// Adds a ProcFunc to the app.
// The instance may use opt to retrieve parameters and is wired
// using the inputs.
func (app *App) Add(fn ProcFunc, opt interface{}, inputs ...Processor) Processor {

	ctx := app.createContext(fn, opt, inputs...)
	ctx.proc = procInstance(ctx)
	return ctx.proc
}

// Closure to genarate a Processor with parameter id and cache.
func procInstance(ctx *Context) Processor {

	return func(key uint64) (Value, error) {

		// Check if the data is in the cache.
		if v, ok := ctx.cache.Get(key); ok {
			//fmt.Printf("DEBUG: CACHE HIT in proc %d\n", id)
			return v, nil
		}
		result, err := ctx.procFunc(key, ctx, ctx.inputs...)
		if err != nil {
			return nil, err
		}
		ctx.cache.Set(key, result)
		return result, nil
	}
}

func (app *App) createContext(fn ProcFunc, opt interface{}, inputs ...Processor) *Context {
	id := len(app.procs)
	ctx := &Context{
		cache:    newCache(),
		Skip:     1,
		procFunc: fn,
		Options:  opt,
		inputs:   inputs,
	}
	app.procs[id] = ctx
	return ctx
}

type Slice struct {
	Start, End uint64
	Data       interface{}
}

func NewSlice(start, end uint64, data interface{}) Value {
	return &Slice{
		Data:  data,
		Start: start,
		End:   end,
	}
}

// A cache (ony for testing.)
// TODO: Implement cache using a circular buffer.
type cache struct {
	m map[uint64]Value
}

func newCache() *cache { return &cache{m: make(map[uint64]Value)} }
func (c *cache) Get(key uint64) (val Value, ok bool) {
	val, ok = c.m[key]
	return
}
func (c *cache) Set(key uint64, val Value) { c.m[key] = val }
