package occult

import "fmt"

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
