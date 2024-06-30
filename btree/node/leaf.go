package node

import (
	"errors"
	"fmt"
)

type LeafNode struct {
	header *NodeHeader
	buf    []byte
}

const NODE_HEADER_SIZE = 100

var ErrNoAvailableSpaceForInsert = errors.New("there is not enough available space for insert")
var ErrFailedToInsertData = errors.New("failed to insert data")
var ErrFailedToInsertKeyDataRef = errors.New("failed to insert key data ref")

func NewEmptyLeafNode(size uint32) *LeafNode {
	buf := make([]byte, size)
	return &LeafNode{
		&NodeHeader{
			LeafNodeType,
			NODE_HEADER_SIZE,
			size,
			0,
		},
		buf,
	}
}

func NewLeafNode(header *NodeHeader, data []byte) *LeafNode {
	return &LeafNode{
		header,
		data,
	}
}

func (l *LeafNode) availableSpace() uint32 {
	return l.header.freeSpaceEndOffset - l.header.freeSpaceStartOffset
}

func (l *LeafNode) Insert(key uint32, data []byte) (*KeyDataReference, error) {
	dataSize := uint32(len(data))
	requiedSpace := KEY_DATA_REF_SIZE + dataSize
	if l.availableSpace() < requiedSpace {
		return nil, ErrNoAvailableSpaceForInsert
	}

	startDataOffset := l.header.freeSpaceEndOffset - dataSize

	// create key
	keyDataRef := &KeyDataReference{
		key,
		startDataOffset,
		dataSize,
	}

	// insert key at the right spot (possibly offset the rest)
	exists, index, err := l.findPositionForKey(key)
	if err != nil {
		return nil, fmt.Errorf("failed to find position of key %d: %v", key, err)
	}

	if exists {
		return nil, errors.New("insert of existing keys is not supported")
	}

	keyData, encodingErr := EncodeKeyDataRef(keyDataRef)
	if encodingErr != nil {
		return nil, fmt.Errorf("failed to encode key data ref: %v", encodingErr)
	}

	if index < l.header.elementsCount {
		// todo: shift
	} else {
		keyDataOffset := index * KEY_DATA_REF_SIZE
		numOfCopiedBytes := copy(l.buf[keyDataOffset:(keyDataOffset+KEY_DATA_REF_SIZE)], keyData)
		if numOfCopiedBytes != KEY_DATA_REF_SIZE {
			return nil, ErrFailedToInsertKeyDataRef
		}
	}

	// copy over buffer
	numOfCopiedBytes := copy(l.buf[startDataOffset:(startDataOffset+dataSize)], data)
	if numOfCopiedBytes != int(dataSize) {
		return nil, ErrFailedToInsertData

	}

	// update header
	l.header.elementsCount += 1
	l.header.freeSpaceStartOffset += KEY_DATA_REF_SIZE
	l.header.freeSpaceEndOffset -= dataSize

	return keyDataRef, nil
}

func (l *LeafNode) getKeyRefByIndex(index uint32) (*KeyDataReference, error) {
	offset := index * KEY_DATA_REF_SIZE
	keyData := l.buf[offset:(offset + KEY_DATA_REF_SIZE)]
	return DecodeKeyDataRef(keyData)
}

func (l *LeafNode) findPositionForKey(key uint32) (bool, uint32, error) {
	start := uint32(0)
	end := l.header.elementsCount
	for start < end {
		middle := (start + end) / 2
		middleKeyDataRef, err := l.getKeyRefByIndex(middle)
		if err != nil {
			return false, 0, err
		}

		if middleKeyDataRef.Key == key {
			return true, middle, nil
		}

		if middleKeyDataRef.Key < key {
			start = middle + 1
		} else {
			end = middle
		}
	}

	return false, start, nil

}
