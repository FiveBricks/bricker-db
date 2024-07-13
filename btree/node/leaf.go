package node

import (
	"errors"
	"fmt"
	"math"

	"golang.org/x/exp/slices"
)

type LeafNode struct {
	Header *NodeHeader
	Buf    []byte
}

type LeafNodeInsertResult struct {
	InsertedKeyDataRef *KeyDataReference
	Metadata           *InsertMetadata
}

func NewEmptyLeafNode(size uint32) *LeafNode {
	buf := make([]byte, size)
	return &LeafNode{
		&NodeHeader{
			LeafNodeType,
			size,
			0,
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

func (l *LeafNode) Insert(key uint32, data []byte) (*LeafNodeInsertResult, error) {
	dataSize := uint32(len(data))
	requiedSpace := KEY_DATA_REF_SIZE + dataSize
	if l.Header.GetAvailableSpace() < requiedSpace {
		// if required space is smaller than half of the node size, we should be able
		// to insert the data after a split
		if requiedSpace < (l.Header.nodeSize / 2) {
			return l.splitAndInsert(key, data)

		}

		return nil, ErrNoAvailableSpaceForInsert
	}

	// find position for the new key
	exists, index, err := FindPositionForKey(l, key)
	if err != nil {
		return nil, fmt.Errorf("failed to find position of key %d: %v", key, err)
	}

	if exists {
		return nil, errors.New("insert of existing keys is not supported")
	}

	keyRef, insertErr := l.insertToIndex(index, key, data)
	if insertErr != nil {
		return nil, insertErr
	}

	return &LeafNodeInsertResult{keyRef, &InsertMetadata{nil}}, nil
}

func (l *LeafNode) insertToIndex(index uint32, key uint32, data []byte) (*KeyDataReference, error) {
	dataSize := uint32(len(data))
	requiedSpace := KEY_DATA_REF_SIZE + dataSize
	if l.Header.GetAvailableSpace() < requiedSpace {
		return nil, ErrNoAvailableSpaceForInsert
	}

	startDataOffset := l.Header.freeSpaceEndOffset - dataSize

	// create key
	keyDataRef := &KeyDataReference{
		key,
		startDataOffset,
		dataSize,
	}

	keyData, encodingErr := EncodeKeyDataRef(keyDataRef)
	if encodingErr != nil {
		return nil, fmt.Errorf("failed to encode key data ref: %v", encodingErr)
	}

	// offset existing keys
	if index < l.Header.elementsCount {
		offsetStart := index * KEY_DATA_REF_SIZE
		offsetEnd := l.Header.elementsCount * KEY_DATA_REF_SIZE
		if copiedBytes := copy(l.Buf[(offsetStart+KEY_DATA_REF_SIZE):], l.Buf[offsetStart:offsetEnd]); copiedBytes != int(offsetEnd-offsetStart) {
			return nil, errors.New("failed to shift existing keys")

		}
	}

	keyDataOffset := index * KEY_DATA_REF_SIZE
	if numOfCopiedBytes := copy(l.Buf[keyDataOffset:(keyDataOffset+KEY_DATA_REF_SIZE)], keyData); numOfCopiedBytes != KEY_DATA_REF_SIZE {
		return nil, ErrFailedToInsertKeyDataRef
	}

	// copy over buffer
	numOfCopiedBytes := copy(l.Buf[startDataOffset:(startDataOffset+dataSize)], data)
	if numOfCopiedBytes != int(dataSize) {
		return nil, ErrFailedToInsertData

	}

	// update header
	l.Header.elementsCount += 1
	l.Header.freeSpaceStartOffset += KEY_DATA_REF_SIZE
	l.Header.freeSpaceEndOffset -= dataSize

	return keyDataRef, nil
}

func (l *LeafNode) append(key uint32, data []byte) (*KeyDataReference, error) {
	index := l.Header.elementsCount
	return l.insertToIndex(index, key, data)
}

func (l *LeafNode) splitAndInsert(key uint32, data []byte) (*LeafNodeInsertResult, error) {
	var keyRefs []*KeyDataReference
	for i := uint32(0); i < l.Header.elementsCount; i++ {
		ref, err := l.getKeyDataRefByIndex(i)
		if err != nil {
			return nil, err
		}

		keyRefs = append(keyRefs, ref)
	}

	var keyRefsCommit []*KeyDataReferenceCommit
	for _, keyRef := range keyRefs {
		keyRefsCommit = append(keyRefsCommit, &KeyDataReferenceCommit{keyRef, true})
	}

	exist, newItemPosition := FindPositionForKeyInRefs(key, keyRefs)
	if exist {
		return nil, errors.New("cannot insert to an existing position")
	}

	newItemKeyRef := &KeyDataReference{key, 0, 0}
	keyRefsCommit = slices.Insert(keyRefsCommit, int(newItemPosition), &KeyDataReferenceCommit{newItemKeyRef, false})

	splitPoint := int32(math.Ceil(float64(len(keyRefsCommit)) / 2))
	splitKey := keyRefsCommit[splitPoint].keyDataRef.Key

	newNode := NewEmptyLeafNode(l.Header.nodeSize)
	newNodeItems := keyRefsCommit[splitPoint:]

	var insertedKeyRef *KeyDataReference
	for _, item := range newNodeItems {
		ref := item.keyDataRef
		var itemData []byte
		if item.committed {
			itemData = l.Buf[ref.Offset:(ref.Offset + ref.Length)]
		} else {
			itemData = data
		}

		appendedKeyRef, appendErr := newNode.append(item.keyDataRef.Key, itemData)
		if appendErr != nil {
			return nil, fmt.Errorf("failed to copy data to new node: %w", appendErr)
		}

		if !item.committed {
			insertedKeyRef = appendedKeyRef
		}
	}

	// delete moved items in old node, starting from back
	newNodesLen := len(newNodeItems)
	for i := newNodesLen - 1; i >= 0; i-- {
		item := newNodeItems[i]
		if item.committed {
			if err := l.deleteLastKeyRef(); err != nil {
				return nil, fmt.Errorf("failed to delete moved key: %w", err)
			}
		}
	}

	// insert new item to old node if needed
	if newItemPosition < uint32(splitPoint) {
		keyRef, insertErr := l.insertToIndex(newItemPosition, key, data)
		if insertErr != nil {
			return nil, insertErr
		}

		insertedKeyRef = keyRef
	}

	return &LeafNodeInsertResult{insertedKeyRef, &InsertMetadata{&SplitMetadata{splitKey, newNode}}}, nil
}

func (l *LeafNode) getKeyDataRefByIndex(index uint32) (*KeyDataReference, error) {
	if !(index < l.GetElementsCount()) {
		return nil, ErrKeyRefAtIndexDoesNotExist
	}
	offset := index * KEY_DATA_REF_SIZE
	keyData := l.Buf[offset:(offset + KEY_DATA_REF_SIZE)]
	return DecodeKeyDataRef(keyData)
}

func (l *LeafNode) GetKeyRefeferenceByIndex(index uint32) (KeyReference, error) {
	// wrapper to accommodate the node interface signature
	return l.getKeyDataRefByIndex(index)
}

func (l *LeafNode) deleteLastKeyRef() error {
	// delete key ref
	l.Header.freeSpaceStartOffset -= KEY_DATA_REF_SIZE
	// l.header.freeSpaceEndOffset += keyRef.Length
	l.Header.elementsCount -= 1

	// todo: remove data & defragment & flush

	return nil
}

func (l *LeafNode) GetElementsCount() uint32 {
	return l.Header.elementsCount
}

func (l *LeafNode) getKeyRefData(keyDataRef *KeyDataReference) []byte {
	return l.Buf[keyDataRef.Offset:(keyDataRef.Offset + keyDataRef.Length)]
}
