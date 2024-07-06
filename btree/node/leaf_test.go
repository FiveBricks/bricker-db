package node

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestInsertIntoLeafNode(t *testing.T) {
	leafSize := uint32(1024)
	leaf := NewEmptyLeafNode(leafSize)
	data := []byte("data")
	key := uint32(1)

	insertResult, insertErr := leaf.Insert(key, data)
	keyDataRef := insertResult.InsertedKeyDataRef
	assert.NoError(t, insertErr)
	assert.Equal(t, key, keyDataRef.Key)
	assert.Equal(t, uint32(len(data)), keyDataRef.Length)
	assert.Equal(t, leafSize-uint32(len(data)), keyDataRef.Offset)

	assert.Equal(t, uint32(1), leaf.GetElementsCount())
}

func TestFindPositionForKey(t *testing.T) {
	leaf := NewEmptyLeafNode(1024)
	data := []byte("data")
	key := uint32(1)

	_, insertErr := leaf.Insert(key, data)
	assert.NoError(t, insertErr)

	key2 := uint32(0)
	exists, index, err := leaf.findPositionForKey(key2)
	assert.NoError(t, err)
	assert.False(t, exists)
	assert.Equal(t, uint32(0), index)
}

func TestInsertBeforeExistingElementIntoLeafNode(t *testing.T) {
	leaf := NewEmptyLeafNode(1024)

	key1Data := []byte("key1Data")
	insert1Result, insertErr := leaf.Insert(uint32(1), key1Data)
	key1DataRef := insert1Result.InsertedKeyDataRef
	assert.NoError(t, insertErr)

	key2Data := []byte("key2Data")
	insert2Result, insert2Err := leaf.Insert(uint32(0), key2Data)
	key2DataRef := insert2Result.InsertedKeyDataRef
	assert.NoError(t, insert2Err)

	// check order
	firstKeyInLeaf, getKeyErr := leaf.getKeyRefByIndex(0)
	assert.NoError(t, getKeyErr)
	assert.Equal(t, key2DataRef, firstKeyInLeaf)

	secondKeyInLeaf, getKeyErr := leaf.getKeyRefByIndex(1)
	assert.NoError(t, getKeyErr)
	assert.Equal(t, key1DataRef, secondKeyInLeaf)

	// check data
	firstKeyData := leaf.getKeyRefData(firstKeyInLeaf)
	assert.Equal(t, key2Data, firstKeyData)
	secondKeyData := leaf.getKeyRefData(secondKeyInLeaf)
	assert.Equal(t, key1Data, secondKeyData)
}
