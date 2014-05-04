// Cache-Oriented Array Processing (COAP)
package coap

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

// A cache (ony for prototyping. a production cache should evict old values, etc)
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
