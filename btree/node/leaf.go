package node

import (
	"errors"
	"fmt"

	"golang.org/x/exp/slices"
)

type LeafNode struct {
	header *NodeHeader
	buf    []byte
}

type LeafNodeInsertResult struct {
	InsertedKeyDataRef *KeyDataReference
	Metadata           *InsertMetadata
}

const NODE_HEADER_SIZE = 100

var ErrNoAvailableSpaceForInsert = errors.New("there is not enough available space for insert")
var ErrFailedToInsertData = errors.New("failed to insert data")
var ErrFailedToInsertKeyDataRef = errors.New("failed to insert key data ref")
var ErrKeyRefAtIndexDoesNotExist = errors.New("Key data reference at given index does not exist")

func NewEmptyLeafNode(size uint32) *LeafNode {
	buf := make([]byte, size)
	return &LeafNode{
		&NodeHeader{
			LeafNodeType,
			size,
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

func (l *LeafNode) Insert(key uint32, data []byte) (*LeafNodeInsertResult, error) {
	dataSize := uint32(len(data))
	requiedSpace := KEY_DATA_REF_SIZE + dataSize
	if l.availableSpace() < requiedSpace {
		// if required space is smaller than half of the node size, we should be able
		// to insert the data after a split
		if requiedSpace < (l.header.nodeSize / 2) {
			return l.splitAndInsert(key, data)

		}

		return nil, ErrNoAvailableSpaceForInsert
	}

	// find position for the new key
	exists, index, err := l.findPositionForKey(key)
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

	keyData, encodingErr := EncodeKeyDataRef(keyDataRef)
	if encodingErr != nil {
		return nil, fmt.Errorf("failed to encode key data ref: %v", encodingErr)
	}

	// offset existing keys
	if index < l.header.elementsCount {
		offsetStart := index * KEY_DATA_REF_SIZE
		offsetEnd := l.header.elementsCount * KEY_DATA_REF_SIZE
		if copiedBytes := copy(l.buf[(offsetStart+KEY_DATA_REF_SIZE):], l.buf[offsetStart:offsetEnd]); copiedBytes != int(offsetEnd-offsetStart) {
			return nil, errors.New("failed to shift existing keys")

		}
	}

	keyDataOffset := index * KEY_DATA_REF_SIZE
	if numOfCopiedBytes := copy(l.buf[keyDataOffset:(keyDataOffset+KEY_DATA_REF_SIZE)], keyData); numOfCopiedBytes != KEY_DATA_REF_SIZE {
		return nil, ErrFailedToInsertKeyDataRef
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

func (l *LeafNode) append(key uint32, data []byte) (*KeyDataReference, error) {
	index := l.header.elementsCount
	return l.insertToIndex(index, key, data)
}

func (l *LeafNode) splitAndInsert(key uint32, data []byte) (*LeafNodeInsertResult, error) {
	fmt.Println("############# inside split")
	var keyRefs []*KeyDataReference
	for i := uint32(0); i < l.header.elementsCount; i++ {
		ref, err := l.getKeyRefByIndex(i)
		if err != nil {
			return nil, err
		}

		keyRefs = append(keyRefs, ref)
	}

	var keyRefsCommit []*KeyDataReferenceCommit
	for _, keyRef := range keyRefs {
		keyRefsCommit = append(keyRefsCommit, &KeyDataReferenceCommit{keyRef, true})
	}

	exist, newItemPosition := l.findPositionForKeyInRefs(key, keyRefs)
	if exist {
		return nil, errors.New("cannot insert to an existing position")
	}

	newItemKeyRef := &KeyDataReference{key, 0, 0}
	keyRefsCommit = slices.Insert(keyRefsCommit, int(newItemPosition), &KeyDataReferenceCommit{newItemKeyRef, false})

	fmt.Printf("############# len: %d\n", len(keyRefsCommit))
	splitPoint := len(keyRefsCommit) / 2
	splitKey := keyRefsCommit[splitPoint].keyDataRef.Key

	newNode := NewEmptyLeafNode(l.header.nodeSize)
	newNodeItems := keyRefsCommit[splitPoint:]

	var insertedKeyRef *KeyDataReference
	for _, item := range newNodeItems {
		ref := item.keyDataRef
		var itemData []byte
		if item.committed {
			itemData = l.buf[ref.Offset:(ref.Offset + ref.Length)]
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

	fmt.Printf("############# new position: %d, split point: %d\n", newItemPosition, splitPoint)
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

func (l *LeafNode) getKeyRefByIndex(index uint32) (*KeyDataReference, error) {
	if !(index < l.GetElementsCount()) {
		return nil, ErrKeyRefAtIndexDoesNotExist
	}
	offset := index * KEY_DATA_REF_SIZE
	keyData := l.buf[offset:(offset + KEY_DATA_REF_SIZE)]
	return DecodeKeyDataRef(keyData)
}

func (l *LeafNode) deleteKeyRefByIndex(index uint32) (*KeyDataReference, error) {
	offset := index * KEY_DATA_REF_SIZE
	keyData := l.buf[offset:(offset + KEY_DATA_REF_SIZE)]
	return DecodeKeyDataRef(keyData)
}

func (l *LeafNode) deleteLastKeyRef() error {
	keyRef, err := l.getKeyRefByIndex(l.header.elementsCount - 1)
	if err != nil {
		return err
	}

	// delete key ref
	l.header.freeSpaceStartOffset -= KEY_DATA_REF_SIZE
	l.header.freeSpaceEndOffset += keyRef.Length
	l.header.elementsCount -= 1

	// todo: remove data
	// todo: flush

	return nil
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

func (l *LeafNode) findPositionForKeyInRefs(key uint32, refs []*KeyDataReference) (bool, uint32) {
	start := uint32(0)
	end := uint32(len(refs))
	for start < end {
		middle := (start + end) / 2
		middleKeyDataRef := refs[middle]

		if middleKeyDataRef.Key == key {
			return true, middle
		}

		if middleKeyDataRef.Key < key {
			start = middle + 1
		} else {
			end = middle
		}
	}

	return false, start
}

func (l *LeafNode) GetElementsCount() uint32 {
	return l.header.elementsCount
}

func (l *LeafNode) getKeyRefData(keyDataRef *KeyDataReference) []byte {
	return l.buf[keyDataRef.Offset:(keyDataRef.Offset + keyDataRef.Length)]
}
