package occult

import "net/rpc"

type node struct {
	nid      int
	rpClient *rpc.Client
}

func (n *node) id() int {
	return n.nid
}

type cluster struct {
	nodes []node
}
