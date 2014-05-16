package occult

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"time"
)

// Here are the functions that handle remote process requests. Abtraction is: give me values
// for indices between start and end for processor instance running on remote node.

// Executes remote synchronous call to target remote process on target node. Returns value.
func (app *App) rpCall(key uint64, procID int, node *Node) (Value, error) {
	vals, err := app.rpCallSlice(key, key+1, procID, node)
	if vals == nil {
		log.Printf("DEBUG CALL err: %s", err)
		return nil, err
	}
	return vals[0], err
}

// Executes remote synchronous call to target remote process on target node. Returns slice.
func (app *App) rpCallSlice(start, end uint64, procID int, node *Node) (vals []Value, err error) {
	args := &RArgs{Start: start, End: end, ProcID: procID}
	reply := RValue{}
	log.Printf("DEBUG CALLSLICE before call:%#v", args)
	err = node.rpClient.Call("RProc.Get", args, &reply)
	if err != nil {
		log.Printf("DEBUG CALLSLICE err: %s", err)
		return nil, fmt.Errorf("rpCall error: %s", err)
	}
	log.Printf("DEBUG CALLSLICE reply:%#v", reply)
	return reply.Vals, nil
}

// Check if remote server is ready.
func rpIsReady(node *Node, ch chan bool) {

	args := 0
	var ready bool
	for !ready {
		log.Printf("checking if server %s is ready", node.Addr)
		err := node.rpClient.Call("RProc.Ready", args, &ready)
		if err != nil {
			log.Printf("waiting for server ready: %s", err)
		}
		time.Sleep(2 * time.Second)
	}
	//ch <- true
	close(ch)
}

// Starts the remote process server.
func (app *App) rpServe(addr string) {
	rp := &RProc{
		app: app,
	}
	rpc.Register(rp)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", addr)
	if e != nil {
		log.Fatalf("listen error: %s", e)
	}
	http.Serve(l, nil)
}

// Returns client for target server address.
func rpClient(addr string) (client *rpc.Client, err error) {
	client, err = rpc.DialHTTP("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("dialing error: %s", err)
	}
	return
}

// Below are the low-level functions to handle inter-process communication.

// Use RPC to request a value to a remote node. The arg is the key range
// and processor instance id. If succesful, returns a slice of values whose
// length is between zero and (End-Start).
type RArgs struct {
	Start, End uint64
	ProcID     int
}

// Returned type for RPC method.
type RValue struct {
	Vals []Value
	// here we can have metadata sent by remote server.
}

// RPC type to get remote values.
type RProc struct {
	app *App
}

// RPC method to get remote values.
func (rp *RProc) Get(args *RArgs, rv *RValue) error {

	n := args.End - args.Start
	rv.Vals = make([]Value, 0, n)
	ctx := rp.app.Context(args.ProcID)
	p := ctx.proc
	idx := args.Start
	log.Printf("DEBUG: Get ctx:%#v, args:%v", ctx, args)
	for ; idx < args.End; idx++ {
		val, err := p(uint64(idx))
		log.Printf("DEBUG: Get idx:%d, val:%v", idx, val)
		if err != nil {
			log.Printf("DEBUG: Get error:%s", err)
			return fmt.Errorf("rpc error: %s", err)
		}
		rv.Vals = append(rv.Vals, val)
	}
	log.Printf("DEBUG: Get args:%#v, rv:%#v", args, rv)
	return nil
}

// Tells client if server is ready to start takign requests.
func (rp *RProc) Ready(args int, ready *bool) error {

	if rp.app.ready {
		*ready = true
	}
	return nil
}
