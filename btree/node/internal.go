package node

import (
	"errors"
	"fmt"
	"math"

	"golang.org/x/exp/slices"
)

const INTERNAL_NODE_SIZE = 4096

type InternalNode struct {
	header *NodeHeader
	buf    []byte
}

type InternalNodeInsertResult struct {
	InsertedKeyPageRef *KeyPageReference
	Metadata           *InsertMetadata
}

func NewEmptyInternalNode(size uint32) *InternalNode {
	buf := make([]byte, size)
	return &InternalNode{
		&NodeHeader{
			InternalNodeType,
			size,
			0,
			size,
			0,
		},
		buf,
	}
}

func NewInternalNode(header *NodeHeader, data []byte) *InternalNode {
	return &InternalNode{
		header,
		data,
	}
}

func (i *InternalNode) Insert(key uint32, pageId uint32) (*InternalNodeInsertResult, error) {
	if i.header.GetAvailableSpace() < KEY_PAGE_REF_SIZE {
		// if required space is smaller than half of the node size, we should be able
		// to insert the data after a split
		if KEY_PAGE_REF_SIZE < (i.header.NodeSize / 2) {
			return i.splitAndInsert(key, pageId)

		}

		return nil, ErrNoAvailableSpaceForInsert
	}

	// find position for the new key
	exists, index, err := FindPositionForKey(i, key)
	if err != nil {
		return nil, fmt.Errorf("failed to find position of key %d: %v", key, err)
	}

	if exists {
		return nil, errors.New("insert of existing keys is not supported")
	}

	var highKeyUpdate *HighKeyUpdate
	if index == i.GetElementsCount() {
		highKeyUpdate = &HighKeyUpdate{key}
	}

	keyRef, insertErr := i.insertToIndex(index, key, pageId)
	if insertErr != nil {
		return nil, insertErr
	}

	return &InternalNodeInsertResult{keyRef, &InsertMetadata{nil, highKeyUpdate}}, nil
}

func (i *InternalNode) UpdateAtIndex(index uint32, key uint32, pageId uint32) (*HighKeyUpdate, error) {
	if !(index < i.GetElementsCount()) {
		return nil, errors.New("failed to update key page ref: does not exist")
	}

	// create key
	keyPageRef := &KeyPageReference{
		key,
		pageId,
	}

	keyData, encodingErr := EncodeKeyPageRef(keyPageRef)
	if encodingErr != nil {
		return nil, fmt.Errorf("failed to encode key data ref: %v", encodingErr)
	}

	offsetStart := index * KEY_PAGE_REF_SIZE

	if numOfCopiedBytes := copy(i.buf[offsetStart:(offsetStart+KEY_PAGE_REF_SIZE)], keyData); numOfCopiedBytes != KEY_PAGE_REF_SIZE {
		return nil, ErrFailedToInsertKeyPageRef
	}

	// if we are updating the high key
	if index == (i.GetElementsCount() - 1) {
		return &HighKeyUpdate{key}, nil
	}

	return nil, nil
}

func (i *InternalNode) insertToIndex(index uint32, key uint32, pageId uint32) (*KeyPageReference, error) {
	if i.header.GetAvailableSpace() < KEY_PAGE_REF_SIZE {
		return nil, ErrNoAvailableSpaceForInsert
	}

	// create key
	keyPageRef := &KeyPageReference{
		key,
		pageId,
	}

	keyData, encodingErr := EncodeKeyPageRef(keyPageRef)
	if encodingErr != nil {
		return nil, fmt.Errorf("failed to encode key data ref: %v", encodingErr)
	}

	// offset existing keys
	offsetStart := index * KEY_PAGE_REF_SIZE
	if index < i.header.ElementsCount {
		offsetEnd := i.header.ElementsCount * KEY_PAGE_REF_SIZE
		if copiedBytes := copy(i.buf[(offsetStart+KEY_PAGE_REF_SIZE):], i.buf[offsetStart:offsetEnd]); copiedBytes != int(offsetEnd-offsetStart) {
			return nil, errors.New("failed to shift existing key page refs")
		}
	}

	if numOfCopiedBytes := copy(i.buf[offsetStart:(offsetStart+KEY_PAGE_REF_SIZE)], keyData); numOfCopiedBytes != KEY_PAGE_REF_SIZE {
		return nil, ErrFailedToInsertKeyPageRef
	}

	// update header
	i.header.ElementsCount += 1
	i.header.FreeSpaceStartOffset += KEY_PAGE_REF_SIZE

	return keyPageRef, nil
}

func (i *InternalNode) GetElementsCount() uint32 {
	return i.header.ElementsCount
}

func (i *InternalNode) GetKeyPageRefByIndex(index uint32) (*KeyPageReference, error) {
	if !(index < i.GetElementsCount()) {
		return nil, ErrKeyRefAtIndexDoesNotExist
	}
	offset := index * KEY_PAGE_REF_SIZE
	keyData := i.buf[offset:(offset + KEY_PAGE_REF_SIZE)]
	return DecodeKeyPageRef(keyData)
}

func (i *InternalNode) GetKeyRefeferenceByIndex(index uint32) (KeyReference, error) {
	// wrapper to accommodate the node interface signature
	return i.GetKeyPageRefByIndex(index)
}

func (i *InternalNode) splitAndInsert(key uint32, pageId uint32) (*InternalNodeInsertResult, error) {
	var keyRefs []*KeyPageReference
	for index := uint32(0); index < i.header.ElementsCount; index++ {
		ref, err := i.GetKeyPageRefByIndex(index)
		if err != nil {
			return nil, err
		}

		keyRefs = append(keyRefs, ref)
	}

	var keyRefsCommit []*KeyPageReferenceCommit
	for _, keyRef := range keyRefs {
		keyRefsCommit = append(keyRefsCommit, &KeyPageReferenceCommit{keyRef, true})
	}

	exist, newItemPosition := FindPositionForKeyInRefs(key, keyRefs)
	if exist {
		return nil, errors.New("cannot insert to an existing position")
	}

	var highKeyUpdate *HighKeyUpdate
	if int(newItemPosition) == len(keyRefs) {
		highKeyUpdate = &HighKeyUpdate{key}
	}

	newItemKeyRef := &KeyPageReference{key, 0}
	keyRefsCommit = slices.Insert(keyRefsCommit, int(newItemPosition), &KeyPageReferenceCommit{newItemKeyRef, false})

	splitPoint := int32(math.Ceil(float64(len(keyRefsCommit)) / 2))
	splitKey := keyRefsCommit[splitPoint].keyPageRef.Key

	newNode := NewEmptyInternalNode(i.header.NodeSize)
	newNodeItems := keyRefsCommit[splitPoint:]

	var insertedKeyRef *KeyPageReference
	for _, item := range newNodeItems {
		ref := item.keyPageRef
		appendedKeyRef, appendErr := newNode.append(ref.Key, ref.PageId)
		if appendErr != nil {
			return nil, fmt.Errorf("failed to copy data to new node: %w", appendErr)
		}

		if !item.committed {
			insertedKeyRef = appendedKeyRef
		}
	}

	// delete moved items in old node, starting from back
	newNodesLen := len(newNodeItems)
	for index := newNodesLen - 1; index >= 0; index-- {
		item := newNodeItems[index]
		if item.committed {
			if err := i.deleteLastKeyRef(); err != nil {
				return nil, fmt.Errorf("failed to delete moved key: %w", err)
			}
		}
	}

	// insert new item to old node if needed
	if newItemPosition < uint32(splitPoint) {
		keyRef, insertErr := i.insertToIndex(newItemPosition, key, pageId)
		if insertErr != nil {
			return nil, insertErr
		}

		insertedKeyRef = keyRef
	}

	return &InternalNodeInsertResult{insertedKeyRef, &InsertMetadata{&SplitMetadata{splitKey, newNode, i}, highKeyUpdate}}, nil
}

func (i *InternalNode) append(key uint32, pageId uint32) (*KeyPageReference, error) {
	index := i.header.ElementsCount
	return i.insertToIndex(index, key, pageId)
}

func (i *InternalNode) deleteLastKeyRef() error {
	// delete key ref
	i.header.FreeSpaceStartOffset -= KEY_PAGE_REF_SIZE
	i.header.ElementsCount -= 1

	// todo: remove data & flush
	return nil
}

func (i *InternalNode) GetHeader() *NodeHeader {
	return i.header
}

func (i *InternalNode) GetBuffer() []byte {
	return i.buf
}

func (i *InternalNode) FindPositionForKey(key uint32) (uint32, *KeyPageReference, error) {
	_, index, err := FindPositionForKey(i, key)
	if err != nil {
		return 0, nil, err
	}

	if index == i.GetElementsCount() {
		index = i.GetElementsCount() - 1
	}

	keyRef, keyRefErr := i.GetKeyPageRefByIndex(index)
	if keyRefErr != nil {
		return 0, nil, keyRefErr
	}

	return index, keyRef, nil
}

func (i *InternalNode) GetMaxKey() (uint32, error) {
	count := i.GetElementsCount()
	if !(count > 0) {
		return 0, errors.New("node does not contain any elements")

	}

	keyRef, keyRefErr := i.GetKeyRefeferenceByIndex(count - 1)
	if keyRefErr != nil {
		return 0, keyRefErr
	}

	return keyRef.GetKey(), nil
}
