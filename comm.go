package occult

import (
	"fmt"
	"net"
	"net/http"
	"net/rpc"
)

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

	rv = &RValue{
		Vals: make([]Value, 0, args.End-args.Start),
	}
	ctx := rp.app.Context(args.ProcID)
	p := ctx.proc
	for idx, _ := range rv.Vals {
		val, err := p(uint64(idx))
		if err != nil {
			return fmt.Errorf("rpc error: %s", err)
		}
		rv.Vals = append(rv.Vals, val)
	}
	return nil
}

// Starts the remote process server.
func (app *App) rpServe(addr string) error {
	rp := &RProc{
		app: app,
	}
	rpc.Register(rp)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", addr)
	if e != nil {
		return fmt.Errorf("listen error: %s", e)
	}
	go http.Serve(l, nil)
	return nil
}

// Returns client for target server address.
func (app *App) rpClient(addr string) (client *rpc.Client, err error) {
	client, err = rpc.DialHTTP("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("dialing error: %s", err)
	}
	return
}

// Executes remote synchronous call to target remote process on target node. Returns value.
func (app *App) rpCall(key uint64, procID int, node *node) (Value, error) {
	vals, err := app.rpCallSlice(key, key+1, procID, node)
	if vals == nil {
		return nil, err
	}
	return vals[0], err
}

// Executes remote synchronous call to target remote process on target node. Returns slice.
func (app *App) rpCallSlice(start, end uint64, procID int, node *node) (vals []Value, err error) {
	args := &RArgs{Start: start, End: end, ProcID: procID}
	var reply RValue
	err = node.rpClient.Call("RProc.Get", args, &reply)
	if err != nil {
		return nil, fmt.Errorf("rpCall error: %s", err)
	}
	return reply.Vals, nil
}
