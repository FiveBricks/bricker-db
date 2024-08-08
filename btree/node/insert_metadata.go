package node

type InsertMetadata struct {
	Split *SplitMetadata
}

type SplitMetadata struct {
	SplitKey    uint32
	CreatedNode Node
	OldNode     Node
}
