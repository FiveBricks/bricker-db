package pager

import (
	"bricker-db/btree/node"
	"bricker-db/utils"
	"encoding/binary"
	"fmt"
)

func EncodeLeafNode(leafNode *node.LeafNode) ([]byte, error) {
	buf := make([]byte, PAGE_SIZE)
	writer := utils.NewFixedSizeSliceWriter(buf)
	if err := binary.Write(writer, binary.LittleEndian, leafNode); err != nil {
		return nil, fmt.Errorf("failed to encode leaf node: %v", err)
	}

	return buf, nil
}
