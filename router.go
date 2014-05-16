package occult

// A router identifies which remote node can do the
// requested work efficiently for a given processor
// instance and range of keys.
type Router interface {
	// Target node for processor instance and key.
	Route(key uint64, procID int) *Node
	// Target node for processor slice.
	RouteSlice(start, end uint64, procID int) *Node
}

// A router implementation that always route to the same node.
type simpleRouter struct{}

func (r *simpleRouter) Route(key uint64, procID int) *Node {
	return &Node{ID: 0}
}

func (r *simpleRouter) RouteSlice(start, end uint64, procID int) *Node {
	return &Node{ID: 0}
}

// A router implementation that assigns nodes based on key ranges.
// Not for practical use but useful to start testing.
type blockRouter struct {
	numNodes  int
	blockSize uint64
	cluster   *Cluster
}

func (r *blockRouter) Route(key uint64, procID int) *Node {
	block := int(key / r.blockSize)
	return r.cluster.Node(block % r.numNodes)
}

func (r *blockRouter) RouteSlice(start, end uint64, procID int) *Node {
	return r.Route(start, procID)
}
