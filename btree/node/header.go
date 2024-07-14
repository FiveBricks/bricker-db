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
	NodeSize             uint32
	FreeSpaceStartOffset uint32
	FreeSpaceEndOffset   uint32
	ElementsCount        uint32
}

func (h *NodeHeader) GetAvailableSpace() uint32 {
	return h.FreeSpaceEndOffset - h.FreeSpaceStartOffset
}

func (h *NodeHeader) Encode() ([]byte, error) {
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
