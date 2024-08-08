package node

type KeyReference interface {
	GetKey() uint32
}

type Node interface {
	GetKeyRefeferenceByIndex(index uint32) (KeyReference, error)
	GetElementsCount() uint32
	GetHeader() *NodeHeader
	GetBuffer() []byte
	GetMaxKey() (uint32, error)
}
