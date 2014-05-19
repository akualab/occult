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
	"time"

	"github.com/golang/glog"
)

const (
	GoMaxProcs      = 2
	DefaultCacheCap = 1000
	NumRetries      = 20
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
	Name     string `yaml:"name"`
	CacheCap uint64 `yaml:"cache_cap"`
	procs    map[int]*Context
	// The node on which this app is running.
	cluster   *Cluster
	router    Router
	isServer  bool
	ready     bool
	terminate chan bool
}

// Creates a new App.
func NewApp(config *Config) *App {
	app := config.App
	app.procs = make(map[int]*Context)
	app.cluster = config.Cluster
	if app.cluster != nil {
		app.router = &blockRouter{
			numNodes:  len(config.Cluster.Nodes),
			blockSize: 200,
			cluster:   app.cluster,
		}
	}
	app.terminate = make(chan bool)
	runtime.GOMAXPROCS(GoMaxProcs)
	return app
}

func (app *App) SetServer(b bool) {
	app.isServer = b
}

// Run app.
// Must be called after adding processors.
func (app *App) Run() {

	if app.cluster == nil {
		return // one node
	}

	// Start local server.
	addr := app.cluster.LocalNode().Addr
	go app.rpServe(addr)
	glog.Infof("server started on address %s", addr)

	// Init clients to connect to remote nodes.
	var err error
	for _, node := range app.cluster.Nodes {
		if node.ID != app.cluster.NodeID {
			for j := 0; ; j++ {
				if j > NumRetries {
					glog.Fatalf("too many retries, can't connect to server addr [%s]", node.Addr)
				}
				glog.Infof("trying to connect to address %s", node.Addr)
				node.rpClient, err = rpClient(node.Addr)
				if err == nil {
					break
				}
				time.Sleep(5 * time.Second)
			}
			glog.Infof("success! conneted to address %s", node.Addr)
		}
	}

	// This node is ready to start working. Need this to make sure we block requests
	// from other nodes before the connections are initialized.
	app.ready = true

	// Wait for all remote nodes to be ready.
	for _, node := range app.cluster.Nodes {
		if node.ID != app.cluster.NodeID {
			glog.Infof("waiting for server %s to be ready", node.Addr)
			ch := make(chan bool)
			rpIsReady(node, ch)
			<-ch
			glog.Infof("server %s is ready!", node.Addr)
		}
	}

	glog.Infof("all remote nodes are ready")

	// Server mode is done here.
	if app.isServer {
		<-app.terminate
	}
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

// Closure to generate a Processor with parameter id and cache.
func (app *App) procInstance(ctx *Context) Processor {

	return func(key uint64) (Value, error) {

		// TODO: optimization
		// We received a request for a given key. In a cluster, we need
		// to determine which node should do the work. Here is where we need
		// to include the logic. We also need to work in batches to reduce the number
		// of requests. Perhaps, we can use a default batch size so when we request
		// work for key we also do the slice up to key+batchSize. If the this is the
		// target node, continue work here.

		// If there is a cluster send work to other nodes.
		if app.cluster != nil {
			// Let router do the magic, tell us where to send the work.
			targetNode := app.router.Route(key, ctx.id)
			if glog.V(5) {
				glog.Infof("send work to target node %#v", targetNode)
			}
			// Skip remote call if we stay on this node.
			if targetNode.ID != app.cluster.NodeID {
				// Finally, do synchronous remote call and return.
				val, err := app.rpCall(key, ctx.id, targetNode)
				return val, err
			}
		}
		// Do local computation.

		// Check if the data is in the cache.
		if v, ok := ctx.cache.get(key); ok {
			if glog.V(7) {
				glog.Infof("cache hit in proc %d\n", ctx.id)
			}
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
			glog.V(4).Infof("worker exiting with err: %s", err)
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
			glog.V(4).Infof("master got nil, n:%d", n)
		} else {
			glog.V(4).Infof("master send out")
			out <- v
		}
		if n == numWorkers {
			glog.V(4).Infof("master closing")
			close(values)
			close(out)
			return
		}
	}
}
