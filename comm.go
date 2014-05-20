package occult

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"time"

	"github.com/golang/glog"
)

// Here are the functions that handle remote process requests. Abtraction is: give me values
// for indices between start and end for processor instance running on remote node.

// Executes remote synchronous call to target remote process on target node. Returns value.
func (app *App) rpCall(key uint64, procID int, node *Node) (Value, error) {
	slice, err := app.rpCallSlice(key, key+1, procID, node)
	if slice.Data == nil {
		glog.Error(err)
		return nil, err
	}
	return slice.Data[0], err
}

// Executes remote synchronous call to target remote process on target node. Returns slice.
func (app *App) rpCallSlice(start, end uint64, procID int, node *Node) (result *Slice, err error) {
	args := &RArgs{Start: start, End: end, ProcID: procID}
	var reply Slice
	err = node.rpClient.Call("RProc.Get", args, &reply)
	if err != nil {
		glog.Error(err)
		return nil, err
	}
	result = &reply
	return result, nil
}

// Check if remote server is ready.
func rpIsReady(node *Node, ch chan bool) {

	args := 0
	var ready bool
	for !ready {
		glog.Infof("checking if server %s is ready", node.Addr)
		err := node.rpClient.Call("RProc.Ready", args, &ready)
		if err != nil {
			glog.Infof("waiting for server ready: %s", err)
		}
		time.Sleep(2 * time.Second)
	}
	close(ch)
}

func rpShutdown(node *Node) {
	args := 0
	var reply bool
	err := node.rpClient.Call("RProc.Shutdown", args, &reply)
	if err != nil {
		glog.Infof("shutdown for node %s failed with error: %s", node.ID, err)
	}
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
//type RValue struct {
//	Vals []Value
// here we can have metadata sent by remote server.
//}

// RPC type to get remote values.
type RProc struct {
	app *App
}

// RPC method to get remote values.
func (rp *RProc) Get(args *RArgs, rv *Slice) error {

	n := int(args.End - args.Start)
	//rv = NewSlice(args.Start, 0, n)
	rv.Offset = args.Start
	rv.Data = make([]Value, 0, n)

	ctx := rp.app.Context(args.ProcID)
	p := ctx.proc
	idx := args.Start
	for ; idx < args.End; idx++ {
		val, err := p(uint64(idx))
		if err != nil {
			glog.Error(err)
			return fmt.Errorf("rpc error: %s", err)
		}
		rv.Data = append(rv.Data, val)
	}
	return nil
}

// Tells client if server is ready to start takign requests.
func (rp *RProc) Ready(args int, ready *bool) error {

	if rp.app.ready {
		*ready = true
	}
	return nil
}

func (rp *RProc) Shutdown(args int, ready *bool) error {

	rp.app.terminate <- true
	return nil
}
