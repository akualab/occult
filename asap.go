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

type App struct {
	Name  string
	Procs []Processor
	ids   map[Processor]uint32
}

func NewApp(name string, procs ...Processor) *App {

	app := &App{Name: name, Procs: procs}
	app.ids = make(map[Processor]uint32)
	for k, v := range procs {
		app.ids[v] = uint32(k)
	}
	return app
}

func (a *App) ID(proc Processor) uint32 {
	return a.ids[proc]
}

type Frame struct {
	Data interface{}
	proc Processor
}

type Slice struct {
	Start, End uint64
	Data       interface{}
}

func NewSlice(start, end uint64, data interface{}) *Slice {
	return &Slice{
		Data:  data,
		Start: start,
		End:   end,
	}
}

// Processing units must implement the Processor interface.
// A Processor can pull data from its inputs and makes the processed data available
// to other processors by using the Get() method.
type Processor interface {
	// Output slice.
	Get(start, end uint64) (*Slice, error)
}

// A cache (not implemented.)
// TODO: Implement cache using a circular buffer.
type cache struct {
}

func (c *cache) Get(start, end uint64) (sl *Slice, ok bool) { return nil, false }
func (c *cache) Set(start, end uint64, sl *Slice)           {}

// Type of functions implementing the actual processing.
type ProcFunc func(start, end uint64, in ...Processor) (*Slice, error)

// Implements the basic functionality of a Processor. Embed in the custom Process struct.
type BaseProcessor struct {
	Process ProcFunc
	Inputs  []Processor
	cache
	name string
}

func (p *BaseProcessor) Get(start, end uint64) (*Slice, error) {

	// Check if the data is in the cache.
	if sl, ok := p.cache.Get(start, end); ok {
		return sl, nil
	}

	result, err := p.Process(start, end, p.Inputs...)
	if err != nil {
		return nil, err
	}
	p.cache.Set(start, end, result)
	return result, nil
}

// Returns processor id.
// The id must be unique and consistent across hosts.
// To generate ids automaticaly, we apply a hash function to a
// string that is composed of teh IDs of teh input processors.
// IDs are computed recursively when they are first requested.
// NOTE: this approach assumes that the topology of the processors
// is static.
// func (p *BaseProcessor) ID() uint64 {

// 	// If the ID is already generated, we are done.
// 	if p.id != 0 {
// 		return p.id
// 	}

// 	// Concatenate IDs from input processors to obtain a unique signature.
// 	instr := bytes.NewBuffer([]byte{})
// 	for _, v := range p.Inputs {
// 		stringid := fmt.Sprintf("%d", v.ID())
// 		instr.WriteString(stringid)
// 		instr.WriteString(reflect.TypeOf(v).String())
// 	}

// 	// Apply hash function and convert result to uint64.
// 	h := sha1.New()
// 	io.WriteString(h, instr.String())

// 	b := h.Sum(nil)
// 	buf := bytes.NewReader(b)
// 	err := binary.Read(buf, binary.LittleEndian, &p.id)
// 	if err != nil {
// 		log.Fatal("binary.Read failed: ", err)
// 	}

// 	return p.id
// }
