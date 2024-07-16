package pager

import "bricker-db/btree/node"

type PagedNode struct {
	Page uint32
	Node node.Node
}

func (p *PagedNode) GetNodeType() node.NodeType {
	return p.Node.GetHeader().NodeType
}
