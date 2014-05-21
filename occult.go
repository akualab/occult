// Copyright (c) 2014 AKUALAB INC., All rights reserved.
//
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Occult: A cache-oriented array processing platform.
package occult

import (
	"errors"
	"os"
	"runtime"
	"runtime/pprof"
	"sync"
	"time"

	"github.com/golang/glog"
)

const (
	DefaultCacheCap          = 2000
	NumRetries               = 20 // Num attempts to connect to other nodes.
	DefaultBlockSize  uint64 = 10
	DefaultNumWorkers        = 2
	DefaultGoMaxProcs        = 2
)

var (
	ErrEndOfArray = errors.New("reached the end of the array")
)

// All processors must be implemented using this function type.
type ProcFunc func(key uint64, ctx *Context) (Value, error)

// A Processor instance.
// Once the processor instance is created, the parameters and inputs cannot
// be changed.
type Processor func(key uint64) (Value, error)

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
	app      *App
}

func (ctx *Context) Inputs() []Processor {
	return ctx.inputs
}

// An App coordinates the execution of a set of processors.
type App struct {
	Name       string `yaml:"name"`
	CacheCap   uint64 `yaml:"cache_cap"`
	NumWorkers int    `yaml:"num_workers"`
	BlockSize  uint64 `yaml:"block_size"`
	NumRetries int    `yaml:"num_retries"`
	GoMaxProcs int    `yaml:"go_max_procs"`
	procs      map[int]*Context
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
	if app.GoMaxProcs == 0 {
		app.GoMaxProcs = DefaultGoMaxProcs
	}
	runtime.GOMAXPROCS(app.GoMaxProcs)
	if app.CacheCap == 0 {
		glog.Warningf("using default cache capacity value of %d", app.CacheCap)
		app.CacheCap = DefaultCacheCap
	}
	if app.NumWorkers == 0 {
		app.NumWorkers = DefaultNumWorkers
	}
	if app.BlockSize == 0 {
		app.BlockSize = DefaultBlockSize
	}
	return app
}

func (app *App) SetServer(b bool) {
	app.isServer = b
	glog.V(2).Infof("setting isServer flag")
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
		glog.Infof("server is running...")
		<-app.terminate
		pprof.StopCPUProfile()
		glog.Flush()
		os.Exit(0)
	}
}

// Shutdown all the servers in the cluster.
func (app *App) Shutdown() {

	if app.cluster == nil {
		return // nothing to shut down.
	}

	glog.Info("shutting down the cluster")
	for _, node := range app.cluster.Nodes {
		if node.ID != app.cluster.NodeID {
			glog.Infof("shutting down server %s", node.Addr)
			rpShutdown(node)
		}
	}
	glog.Info("shutting down completed")
}

func (app *App) Context(id int) *Context {
	return app.procs[id]
}

// The value returned by Processors.
type Value interface{}

// A slice of values.
type Slice struct {
	Offset uint64
	Data   []Value
}

// Creates a new Slice.
func NewSlice(start uint64, size, cap int) *Slice {
	return &Slice{
		Offset: start,
		Data:   make([]Value, size, cap),
	}
}

// Converts Values to a Slice.
func ToSlice(key uint64, vals ...Value) *Slice {
	s := NewSlice(key, len(vals), len(vals))
	for _, v := range vals {
		s.Data = append(s.Data, v)
	}
	return s
}

// The length of the Slice.
func (s *Slice) Length() int {
	return len(s.Data)
}

// The offset for this Slice.
func (s *Slice) Start() uint64 {
	return s.Offset
}

// Offset position after the last value of this Slice.
func (s *Slice) End() uint64 {
	return s.Offset + uint64(len(s.Data))
}

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
		// TODO: consider using a circular buffer. For now using LRU.
		cache:    newCache(app.CacheCap),
		procFunc: fn,
		Options:  opt,
		inputs:   inputs,
		id:       id,
		app:      app,
	}
	app.procs[id] = ctx
	return ctx
}

// Closure to generate a Processor with parameter id and cache.
func (app *App) procInstance(ctx *Context) Processor {

	return func(key uint64) (Value, error) {

		var err error
		var vals *Slice

		// First, we check if the data is already in the cache.
		if v, ok := ctx.cache.get(key); ok {
			if glog.V(7) {
				glog.Infof("cache hit in proc %d\n", ctx.id)
			}
			return v, nil
		}

		// Check if we need to send teh work to a remote node.
		if app.cluster != nil {
			// Let router do the magic, tell us where to send the work.
			targetNode := app.router.Route(key, ctx.id)

			if glog.V(5) {
				if targetNode.ID != app.cluster.NodeID {
					glog.Infof("send work to target node key:%d, procid:%d,  %#v",
						int(key), ctx.id, targetNode)
				} else {
					glog.Infof("do work on local node key:%d, procid:%d", int(key),
						ctx.id)
				}
			}

			// Skip remote call if work is done by this node.
			if targetNode.ID != app.cluster.NodeID {

				// Prepare to send work to remote node and wait for results.

				// For efficiency, we request a block of keys at a time.
				// Key are mapped to blocks. blockStart() returns the start of the block.
				start := blockStart(key, app.BlockSize)
				// Get the slice from the remote node.
				vals, err = app.rpCallSlice(start, start+app.BlockSize, ctx.id, targetNode)
				if err != nil || vals.Length() == 0 {
					return nil, err
				}
				// Save the slice in the cache.
				ctx.cache.setSlice(start, vals)
				// Return only the value for key requested (not the slice).
				// blockIndex() maps the requested key to the slice index.
				return vals.Data[blockIndex(key, app.BlockSize)], nil
			}
		}

		// Do local computation.
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

// MapAll applies the processor to the processor values
// with key range {start..}.
func (p Processor) MapAll(start uint64, ctx *Context) chan Value {
	out := make(chan Value, ctx.app.NumWorkers)
	go master(p, ctx.app.NumWorkers, ctx.app.BlockSize, out)
	return out
}

// Provides keys to workers.
type counter struct {
	k    uint64
	size uint64
	sync.Mutex
}

// Safely returns the start of the next block.
func (c *counter) block() uint64 {
	c.Lock()
	defer c.Unlock()
	v := c.k
	c.k += c.size
	return v
}

// Worker does work for block of keys obtained (safely) from counter.
func (c *counter) worker(p Processor, values chan Value) {
	for {
		start := c.block()
		for key := start; key < start+c.size; key++ {
			v, err := p(key)
			if err != nil {
				glog.V(4).Infof("worker exiting with err: %s", err)
				values <- nil
				return
			}
			values <- v
		}
	}
}

// Coordinate workers.
// TODO: num workers for local vs. remote work is the same.
// We need separate params because local depends on the number of cores
// and remote depends on the network topology. To do this we will
// need to determine local vs. remote upstream instead of downstream
// from here.
func master(p Processor, numWorkers int, size uint64, out chan Value) {

	//values := make(chan Value)
	values := make(chan Value, 1000)
	cnt := counter{size: size}
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

// Returns the index in a block given key and block size.
func blockIndex(key, size uint64) int {
	return int(key % size)
}

// Returns the index of a block given block size and key.
func blockStart(key, size uint64) uint64 {
	return (key / size) * size
}
