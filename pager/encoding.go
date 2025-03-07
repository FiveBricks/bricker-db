package pager

import (
	"bricker-db/btree/node"
	"bricker-db/utils"
	"fmt"
)

func EncodeNode(node node.Node) ([]byte, error) {
	buf := make([]byte, PAGE_SIZE)
	writer := utils.NewFixedSizeSliceWriter(buf)

	headerData, headerErr := node.GetHeader().Encode()
	if headerErr != nil {
		return nil, fmt.Errorf("failed to encode leaf node header: %w", headerErr)
	}

	if _, err := writer.Write(headerData); err != nil {
		return nil, fmt.Errorf("failed to write leaf node header: %w", err)
	}

	if _, err := writer.Write(node.GetBuffer()); err != nil {
		return nil, fmt.Errorf("failed to write leaf node buffer: %w", err)
	}

	return buf, nil
}

func DecodeNode(buf []byte) (node.Node, error) {
	headerData := buf[:node.NODE_HEADER_SIZE]
	header, headerErr := node.NewNodeHeaderFromBuffer(headerData)
	if headerErr != nil {
		return nil, fmt.Errorf("failed to decode node: %w", headerErr)
	}

	switch header.NodeType {
	case node.LeafNodeType:
		return node.NewLeafNode(header, buf[node.NODE_HEADER_SIZE:]), nil
	case node.InternalNodeType:
		return node.NewInternalNode(header, buf[node.NODE_HEADER_SIZE:]), nil
	default:
		return nil, fmt.Errorf("unexpected node type: %v", header.NodeType)
	}
}
