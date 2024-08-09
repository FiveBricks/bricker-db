package node

type InsertMetadata struct {
	Split   *SplitMetadata
	HighKey *HighKeyUpdate
}

type SplitMetadata struct {
	SplitKey    uint32
	CreatedNode Node
	OldNode     Node
}

type HighKeyUpdate struct {
	NewHighKey uint32
}
