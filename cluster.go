package occult

import "net/rpc"

type Node struct {
	ID       int    `yaml:"id"`
	Addr     string `yaml:"addr"`
	rpClient *rpc.Client
}

type Cluster struct {
	Name string
	// All nodes in teh cluster.
	Nodes []*Node `yaml:"nodes"`
	// The local node ID.
	NodeID int
}

// Returns true if node id is the local node.
func (c *Cluster) IsLocal(id int) bool {
	if id == c.NodeID {
		return true
	}
	return false
}

// Returns Node for node id.
func (c *Cluster) Node(id int) *Node {

	for _, v := range c.Nodes {
		if v.ID == id {
			return v
		}
	}
	return nil
}

// Returns Node for node id.
func (c *Cluster) LocalNode() *Node {
	return c.Node(c.NodeID)
}
