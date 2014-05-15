package occult

// A router identifies which remote node can do the
// requested work efficiently for a given processor
// instance and range of keys.
type router interface {
	// Target node for processor instance and key.
	route(key uint64, procID int) *node
	// Target node for processor slice.
	routeSlice(start, end uint64, procID int) *node
}

// A router implementation that always route to the same node.
type simpleRouter struct{}

func (r *simpleRouter) route(key uint64, procID int) *node {
	return &node{nid: 0}
}

func (r *simpleRouter) routeSlice(start, end uint64, procID int) *node {
	return &node{nid: 0}
}

// A router implementation that assigns nodes based on key ranges.
// Not for practical use but useful to start testing.
type blockRouter struct {
	numNodes  int
	blockSize uint64
}

func (r *blockRouter) route(key uint64, procID int) *node {
	block := int(key / r.blockSize)
	return &node{nid: block % r.numNodes}
}

func (r *blockRouter) routeSlice(start, end uint64, procID int) *node {
	return r.route(start, procID)
}
