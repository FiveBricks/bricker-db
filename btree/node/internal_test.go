package node

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestInsertIntoInternalNode(t *testing.T) {
	nodeSize := uint32(1024)
	node := NewEmptyInternalNode(nodeSize)
	key := uint32(1)
	pageId := uint32(3)

	insertResult, insertErr := node.Insert(key, pageId)
	keyPageRef := insertResult.InsertedKeyPageRef
	assert.NoError(t, insertErr)
	assert.Equal(t, key, keyPageRef.Key)
	assert.Equal(t, pageId, keyPageRef.PageId)
	assert.Equal(t, uint32(1), node.GetElementsCount())
}

func TestInsertBeforeExistingElementIntoInternalNode(t *testing.T) {
	node := NewEmptyInternalNode(1024)

	key1Page := uint32(5)
	insert1Result, insertErr := node.Insert(uint32(1), key1Page)
	assert.NoError(t, insertErr)
	key1PageRef := insert1Result.InsertedKeyPageRef

	key2Page := uint32(3)
	insert2Result, insert2Err := node.Insert(uint32(0), key2Page)
	assert.NoError(t, insert2Err)
	key2PageRef := insert2Result.InsertedKeyPageRef

	// check order
	firstKeyInNode, getKeyErr := node.getKeyPageRefByIndex(0)
	assert.NoError(t, getKeyErr)
	assert.Equal(t, key2PageRef, firstKeyInNode)
	assert.Equal(t, key2Page, firstKeyInNode.PageId)

	secondKeyInNode, getKeyErr := node.getKeyPageRefByIndex(1)
	assert.NoError(t, getKeyErr)
	assert.Equal(t, key1PageRef, secondKeyInNode)
	assert.Equal(t, key1Page, secondKeyInNode.PageId)
}

func TestInsertAndSplitIntoInternalNode(t *testing.T) {
	node := NewEmptyInternalNode(250)

	key1 := uint32(2)
	key1Page := uint32(3)
	insert1Result, insertErr := node.Insert(key1, key1Page)
	assert.NoError(t, insertErr)
	assert.Nil(t, insert1Result.Metadata.Split)
	key1PageRef := insert1Result.InsertedKeyPageRef

	key2 := uint32(0)
	key2Page := uint32(5)
	insert2Result, insert2Err := node.Insert(key2, key2Page)
	key2PageRef := insert2Result.InsertedKeyPageRef
	assert.NoError(t, insert2Err)
	assert.Nil(t, insert2Result.Metadata.Split)

	key3 := uint32(1)
	key3Page := uint32(7)
	insert3Result, insert3Err := node.Insert(key3, key3Page)
	key3PageRef := insert3Result.InsertedKeyPageRef
	assert.NoError(t, insert3Err)
	assert.NotNil(t, insert3Result.Metadata.Split)

	// check old leaf
	assert.Equal(t, uint32(2), node.GetElementsCount())
	firstKeyInOldLeaf, getKeyErr := node.getKeyPageRefByIndex(0)
	assert.NoError(t, getKeyErr)
	assert.Equal(t, key2PageRef, firstKeyInOldLeaf)
	assert.Equal(t, key2Page, firstKeyInOldLeaf.PageId)

	secondKeyInOldLeaf, getKeyErr := node.getKeyPageRefByIndex(1)
	assert.NoError(t, getKeyErr)
	assert.Equal(t, key3PageRef, secondKeyInOldLeaf)
	assert.Equal(t, key3Page, secondKeyInOldLeaf.PageId)

	_, getKey2Err := node.getKeyPageRefByIndex(2)
	assert.ErrorIs(t, ErrKeyRefAtIndexDoesNotExist, getKey2Err)

	// check new leaf
	newLeaf := insert3Result.Metadata.Split.CreatedNode.(*InternalNode)
	assert.Equal(t, uint32(1), newLeaf.GetElementsCount())
	firstKeyInNewLeaf, getKeyInNewLeafErr := newLeaf.getKeyPageRefByIndex(0)
	assert.NoError(t, getKeyInNewLeafErr)
	assert.Equal(t, key1PageRef, firstKeyInNewLeaf)
	assert.Equal(t, key1Page, firstKeyInNewLeaf.PageId)

	_, getKey2InNewLeafErr := newLeaf.getKeyPageRefByIndex(1)
	assert.ErrorIs(t, ErrKeyRefAtIndexDoesNotExist, getKey2InNewLeafErr)
}

func TestInternalNodeFindPositionForKey(t *testing.T) {
	node := NewEmptyInternalNode(1024)
	key1 := uint32(3)
	_, insertErr := node.Insert(key1, uint32(0))
	assert.NoError(t, insertErr)

	key2 := uint32(5)
	_, insert2Err := node.Insert(key2, uint32(1))
	assert.NoError(t, insert2Err)

	_, position, findErr := node.FindPositionForKey(uint32(1))
	assert.NoError(t, findErr)
	assert.Equal(t, key1, position.Key)

	_, position2, find2Err := node.FindPositionForKey(uint32(7))
	assert.NoError(t, find2Err)
	assert.Equal(t, key2, position2.Key)
}
