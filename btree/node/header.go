package node

import (
	"bricker-db/utils"
	"bytes"
	"encoding/binary"
	"fmt"
)

type NodeType uint32

const (
	InternalNodeType NodeType = iota
	LeafNodeType
)

const NODE_HEADER_SIZE = 100

type NodeHeader struct {
	NodeType             NodeType
	nodeSize             uint32
	freeSpaceStartOffset uint32
	freeSpaceEndOffset   uint32
	elementsCount        uint32
}

func (h *NodeHeader) GetAvailableSpace() uint32 {
	return h.freeSpaceEndOffset - h.freeSpaceStartOffset
}

func (h *NodeHeader) Encoded() ([]byte, error) {
	buf := make([]byte, NODE_HEADER_SIZE)
	writer := utils.NewFixedSizeSliceWriter(buf)

	if err := binary.Write(writer, binary.LittleEndian, h); err != nil {
		return nil, fmt.Errorf("failed to encode node header: %w", err)
	}

	return buf, nil
}

func NewNodeHeaderFromBuffer(buf []byte) (*NodeHeader, error) {
	reader := bytes.NewReader(buf)
	header := &NodeHeader{}

	if err := binary.Read(reader, binary.LittleEndian, header); err != nil {
		return nil, fmt.Errorf("failed to decode node header: %w", err)
	}

	return header, nil
}
