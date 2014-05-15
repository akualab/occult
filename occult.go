// Copyright (c) 2014 AKUALAB INC., All rights reserved.
//
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Occult: A cache-oriented array processing platform.
package occult

import (
	"errors"
	"runtime"
	"sync"
)

const (
	GoMaxProcs       = 2
	DefaultCacheCap  = 1000
	DefaultRPAddress = ":131313"
)

var (
	ErrEndOfArray = errors.New("reached the end of the array")
)

// All processors must be implemented using this function type.
type ProcFunc func(key uint64, ctx *Context) (Value, error)

// The context provides internal information for processor instances.
// Each processor instance has a context.
type Context struct {
	// The Options field is made available to apps to pass parameters
	// to proc instances in any way they want.
	Options interface{}
	// Uniquely identifies a processor instance in a node.
	// A proc instance has the same id in all cluster nodes.
	id       int
	cache    *cache
	procFunc ProcFunc
	proc     Processor
	inputs   []Processor
	isSource bool
}

func (ctx *Context) Inputs() []Processor {
	return ctx.inputs
}

// An App coordinates the execution of a set of processors.
type App struct {
	Name        string
	CacheCap    uint64
	procs       map[int]*Context
	node        *node
	remoteNodes map[int]*node
	router      router
}

// Creates a new App.
func NewApp(name string) *App {

	app := &App{
		Name:     name,
		CacheCap: DefaultCacheCap,
		procs:    make(map[int]*Context),
		node:     &node{nid: 0},
		router:   &simpleRouter{},
	}

	runtime.GOMAXPROCS(GoMaxProcs)
	return app
}

// Sets cache capacity.
func (app *App) SetCacheCap(c uint64) {
	app.CacheCap = c
}

// Initializes app.
// Must be called after adding processors and before execution.
func (app *App) Init() {
	// TODO: build graph of processors. We will use it to optimize
	// the cluster and route requests to nodes.

	// TODO: set nodeID. (Each cluster node must have a unique ID.)
}

func (app *App) Context(id int) *Context {
	return app.procs[id]
}

// The value returned by Processors.
type Value interface{}

// A Processor instance.
// Once the processor instance is created, the parameters and inputs cannot
// be changed.
type Processor func(key uint64) (Value, error)

// Same as Add but indicating that this is a presistent source.
// The system will attempt to use the same cluster node for a given key. This
// affinity will increase the cache hit rate and minimize reads from the persistent
// source.
func (app *App) AddSource(fn ProcFunc, opt interface{}, inputs ...Processor) Processor {

	ctx := app.createContext(fn, opt, inputs...)
	ctx.isSource = true
	ctx.proc = app.procInstance(ctx)
	return ctx.proc
}

// Adds a ProcFunc to the app.
// The instance may use opt to retrieve parameters and is wired
// using the inputs.
func (app *App) Add(fn ProcFunc, opt interface{}, inputs ...Processor) Processor {

	ctx := app.createContext(fn, opt, inputs...)
	ctx.proc = app.procInstance(ctx)
	return ctx.proc
}

// Returns the result of a remote execution.
func (app *App) remote(key uint64, procID int) (Value, error) {
	return nil, nil
}

// Closure to generate a Processor with parameter id and cache.
func (app *App) procInstance(ctx *Context) Processor {

	return func(key uint64) (Value, error) {

		// TODO: implement remote execution.
		// We received a request for a given key. In a cluster, we need
		// to determine which node should do the work. Here is where we need
		// to include the logic. We also need to work in batches to reduce the number
		// of requests. Perhaps, we can use a default batch size so when we request
		// work for key we also do the slice up to key+batchSize. If the this is the
		// target node, continue work here.
		if app.router.route(key, ctx.id).id() != app.node.id() {
			val, err := app.remote(key, ctx.id)
			return val, err
		}

		// Do local computation.

		// Check if the data is in the cache.
		if v, ok := ctx.cache.get(key); ok {
			//fmt.Printf("DEBUG: CACHE HIT in proc %d\n", ctx.id)
			return v, nil
		}
		result, err := ctx.procFunc(key, ctx)
		if err != nil {
			return nil, err
		}
		ctx.cache.set(key, result)
		return result, nil
	}
}

func (app *App) createContext(fn ProcFunc, opt interface{}, inputs ...Processor) *Context {
	id := len(app.procs)
	ctx := &Context{
		// TODO: consider implement cache using a circular buffer. For now using LRU.
		cache:    newCache(app.CacheCap),
		procFunc: fn,
		Options:  opt,
		inputs:   inputs,
		id:       id,
	}
	app.procs[id] = ctx
	return ctx
}

// Map applies the processor to the key range {start..end}.
// Returns a slice of Values of length (end-start).
func (p Processor) Map(start, end uint64) (values []Value, err error) {

	values = make([]Value, end-start)
	for k, _ := range values {
		values[k], err = p(uint64(k))
		if err != nil {
			return
		}
	}
	return
}

// MapAllN applies the processor to the key range {start..}.
// Divides the work among N workers to take advantage
// of multicore CPUs.
func (p Processor) MapAllN(start uint64, numWorkers int) chan Value {

	out := make(chan Value, numWorkers)
	go master(p, numWorkers, out)
	return out
}

// Same as MapAllN but gets the number of workers using
// runtime.NumCPU().
func (p Processor) MapAll(start uint64) chan Value {
	return p.MapAllN(start, runtime.NumCPU())
}

// Provides keys to workers.
type counter struct {
	k uint64
	sync.Mutex
}

// Safely returns the next key to workers.
func (c *counter) key() uint64 {
	c.Lock()
	defer c.Unlock()
	v := c.k
	c.k++
	return v
}

// Worker does work for key obtained (safely) from counter.
func (c *counter) worker(p Processor, values chan Value) {

	for {
		k := c.key()
		v, err := p(k)
		if err != nil {
			//log.Printf("worker exiting with err: %s", err)
			values <- nil
			return
		}
		values <- v
	}
}

// Coordinate workers.
func master(p Processor, numWorkers int, out chan Value) {

	values := make(chan Value)
	cnt := counter{}
	for i := 0; i < numWorkers; i++ {
		go cnt.worker(p, values)
	}

	// TODO: add timeout
	n := 0
	for {
		v := <-values
		if v == nil {
			n++ // increment when worker finishes.
		} else {
			out <- v
		}
		if n == numWorkers {
			close(values)
			close(out)
			return
		}
	}
}
