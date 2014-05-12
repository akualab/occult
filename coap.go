// Copyright (c) 2014 AKUALAB INC., All rights reserved.
//
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Cache-Oriented Array Processing (COAP)
package coap

import (
	"errors"
	"runtime"
	"sync"
)

const (
	GoMaxProcs = 2
)

var (
	ErrEndOfArray = errors.New("reached the end of the array")
)

// All processors must be implemented using this function type.
type ProcFunc func(key uint64, ctx *Context) (Value, error)

// The context provides internal information for processor instances.
// The Options field can be used to pass parameters to processors.
type Context struct {
	Options  interface{}
	Skip     int
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
	Name   string
	procs  map[int]*Context
	nodeID int
}

// Creates a new App.
func NewApp(name string) *App {

	app := &App{Name: name}
	app.procs = make(map[int]*Context)
	runtime.GOMAXPROCS(GoMaxProcs)
	return app
}

// Initializes app.
// Must be called after adding processors and before execution.
func (app *App) Init() {
	// TODO: build graph of processors. We will use it to optimize
	// the cluster and route requests to nodes.

	// TODO: set nodeID. (Each cluster node must have a unique ID.)
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

// Returns the target node ID for key in context.
// TODO: not implemented.
func (app *App) targetNode(key uint64, ctx *Context) int {
	return 0
}

// Returns the result of a remote execution.
func (app *App) remote(key uint64, processID int) (Value, error) {
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
		if app.targetNode(key, ctx) != app.nodeID { // this is a placeholder!
			val, err := app.remote(key, ctx.id)
			return val, err
		}

		// Do local computation.

		// Check if the data is in the cache.
		if v, ok := ctx.cache.Get(key); ok {
			//fmt.Printf("DEBUG: CACHE HIT in proc %d\n", ctx.id)
			return v, nil
		}
		result, err := ctx.procFunc(key, ctx)
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

// A cache (ony for prototyping. a production cache should evict old values, etc)
// TODO: Implement cache using a circular buffer.
type cache struct {
	m map[uint64]Value
	sync.Mutex
}

func newCache() *cache { return &cache{m: make(map[uint64]Value)} }
func (c *cache) Get(key uint64) (val Value, ok bool) {
	c.Lock()
	defer c.Unlock()
	val, ok = c.m[key]
	return
}
func (c *cache) Set(key uint64, val Value) {
	c.Lock()
	defer c.Unlock()
	c.m[key] = val
}
