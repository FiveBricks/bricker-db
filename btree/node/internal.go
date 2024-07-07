package node

type InternalNode struct {
	header *NodeHeader
	buf    []byte
}

func NewEmptyInternalNode(size uint32) *InternalNode {
	buf := make([]byte, size)
	return &InternalNode{
		&NodeHeader{
			InternalNodeType,
			size,
			NODE_HEADER_SIZE,
			size,
			0,
		},
		buf,
	}
}

func NewInternalNode(header *NodeHeader, data []byte) *InternalNode {
	return &InternalNode{
		header,
		data,
	}
}
