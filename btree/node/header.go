package node

type NodeType uint32

const (
	InternalNodeType NodeType = iota
	LeafNodeType
)

type NodeHeader struct {
	nodeType             NodeType
	nodeSize             uint32
	freeSpaceStartOffset uint32
	freeSpaceEndOffset   uint32
	elementsCount        uint32
}
