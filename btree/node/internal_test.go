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
