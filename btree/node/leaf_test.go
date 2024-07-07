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

func TestInsertBeforeExistingElementIntoLeafNode(t *testing.T) {
	leaf := NewEmptyLeafNode(1024)

	key1Data := []byte("key1Data")
	insert1Result, insertErr := leaf.Insert(uint32(1), key1Data)
	assert.NoError(t, insertErr)
	key1DataRef := insert1Result.InsertedKeyDataRef

	key2Data := []byte("key2Data")
	insert2Result, insert2Err := leaf.Insert(uint32(0), key2Data)
	assert.NoError(t, insert2Err)
	key2DataRef := insert2Result.InsertedKeyDataRef

	// check order
	firstKeyInLeaf, getKeyErr := leaf.getKeyDataRefByIndex(0)
	assert.NoError(t, getKeyErr)
	assert.Equal(t, key2DataRef, firstKeyInLeaf)

	secondKeyInLeaf, getKeyErr := leaf.getKeyDataRefByIndex(1)
	assert.NoError(t, getKeyErr)
	assert.Equal(t, key1DataRef, secondKeyInLeaf)

	// check data
	firstKeyData := leaf.getKeyRefData(firstKeyInLeaf)
	assert.Equal(t, key2Data, firstKeyData)
	secondKeyData := leaf.getKeyRefData(secondKeyInLeaf)
	assert.Equal(t, key1Data, secondKeyData)
}

func TestInsertAndSplit(t *testing.T) {
	leaf := NewEmptyLeafNode(250)

	key1Data := []byte("key1Data")
	insert1Result, insertErr := leaf.Insert(uint32(1), key1Data)
	assert.NoError(t, insertErr)
	assert.Nil(t, insert1Result.Metadata.Split)
	key1DataRef := insert1Result.InsertedKeyDataRef

	key2Data := []byte("key2Data")
	insert2Result, insert2Err := leaf.Insert(uint32(0), key2Data)
	assert.NoError(t, insert2Err)
	assert.NotNil(t, insert2Result.Metadata.Split)
	assert.Equal(t, key1DataRef.Key, insert2Result.Metadata.Split.SplitKey)
	key2DataRef := insert2Result.InsertedKeyDataRef
	assert.NotNil(t, key2DataRef)

	// check old leaf
	assert.Equal(t, uint32(1), leaf.GetElementsCount())
	firstKeyInOldLeaf, getKeyErr := leaf.getKeyDataRefByIndex(0)
	assert.NoError(t, getKeyErr)
	assert.Equal(t, key2DataRef, firstKeyInOldLeaf)
	firstKeyInOldLeafData := leaf.getKeyRefData(firstKeyInOldLeaf)
	assert.Equal(t, key2Data, firstKeyInOldLeafData)

	_, getKey2Err := leaf.getKeyDataRefByIndex(1)
	assert.ErrorIs(t, ErrKeyRefAtIndexDoesNotExist, getKey2Err)

	// check new leaf
	newLeaf := insert2Result.Metadata.Split.CreatedNode
	assert.Equal(t, uint32(1), newLeaf.GetElementsCount())
	firstKeyInNewLeaf, getKeyInNewLeafErr := newLeaf.getKeyDataRefByIndex(0)
	assert.NoError(t, getKeyInNewLeafErr)
	assert.Equal(t, key1DataRef, firstKeyInNewLeaf)
	firstKeyInNewLeafData := newLeaf.getKeyRefData(firstKeyInNewLeaf)
	assert.Equal(t, key1Data, firstKeyInNewLeafData)

	_, getKey2InNewLeafErr := newLeaf.getKeyDataRefByIndex(1)
	assert.ErrorIs(t, ErrKeyRefAtIndexDoesNotExist, getKey2InNewLeafErr)
}
