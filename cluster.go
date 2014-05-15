package occult

type node struct {
	nid int
}

func (n *node) id() int {
	return n.nid
}

type cluster struct {
	nodes []node
}
