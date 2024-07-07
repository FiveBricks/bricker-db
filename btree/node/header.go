package node

type NodeType uint32

const (
	InternalNodeType NodeType = iota
	LeafNodeType
)

const NODE_HEADER_SIZE = 100

type NodeHeader struct {
	nodeType             NodeType
	nodeSize             uint32
	freeSpaceStartOffset uint32
	freeSpaceEndOffset   uint32
	elementsCount        uint32
}

func (h *NodeHeader) GetAvailableSpace() uint32 {
	return h.freeSpaceEndOffset - h.freeSpaceStartOffset
}
