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
	firstKeyInLeaf, getKeyErr := leaf.GetKeyDataRefByIndex(0)
	assert.NoError(t, getKeyErr)
	assert.Equal(t, key2DataRef, firstKeyInLeaf)

	secondKeyInLeaf, getKeyErr := leaf.GetKeyDataRefByIndex(1)
	assert.NoError(t, getKeyErr)
	assert.Equal(t, key1DataRef, secondKeyInLeaf)

	// check data
	firstKeyData := leaf.GetKeyRefData(firstKeyInLeaf)
	assert.Equal(t, key2Data, firstKeyData)
	secondKeyData := leaf.GetKeyRefData(secondKeyInLeaf)
	assert.Equal(t, key1Data, secondKeyData)
}

func TestInsertAndSplit(t *testing.T) {
	leaf := NewEmptyLeafNode(250)

	key1Data := []byte("key1Data")
	insert1Result, insertErr := leaf.Insert(uint32(2), key1Data)
	assert.NoError(t, insertErr)
	assert.Nil(t, insert1Result.Metadata.Split)
	key1DataRef := insert1Result.InsertedKeyDataRef

	key2Data := []byte("key2Data")
	insert2Result, insert2Err := leaf.Insert(uint32(0), key2Data)
	assert.NoError(t, insert2Err)
	assert.Nil(t, insert2Result.Metadata.Split)
	key2DataRef := insert2Result.InsertedKeyDataRef

	key3Data := []byte("ke32Data")
	insert3Result, insert3Err := leaf.Insert(uint32(1), key3Data)
	assert.NoError(t, insert3Err)
	assert.NotNil(t, insert3Result.Metadata.Split)
	key3DataRef := insert3Result.InsertedKeyDataRef
	assert.Equal(t, key1DataRef.Key, insert3Result.Metadata.Split.SplitKey)

	// check old leaf
	assert.Equal(t, uint32(2), leaf.GetElementsCount())
	firstKeyInOldLeaf, getKeyErr := leaf.GetKeyDataRefByIndex(0)
	assert.NoError(t, getKeyErr)
	assert.Equal(t, key2DataRef, firstKeyInOldLeaf)
	assert.Equal(t, key2DataRef.Key, uint32(0))
	firstKeyInOldLeafData := leaf.GetKeyRefData(firstKeyInOldLeaf)
	assert.Equal(t, key2Data, firstKeyInOldLeafData)

	secondKeyInOldLeaf, getKeyErr := leaf.GetKeyDataRefByIndex(1)
	assert.NoError(t, getKeyErr)
	assert.Equal(t, key3DataRef, secondKeyInOldLeaf)
	secondKeyInOldLeafData := leaf.GetKeyRefData(secondKeyInOldLeaf)
	assert.Equal(t, key3Data, secondKeyInOldLeafData)

	_, getKey3Err := leaf.GetKeyDataRefByIndex(2)
	assert.ErrorIs(t, ErrKeyRefAtIndexDoesNotExist, getKey3Err)

	// check new leaf
	newLeaf := insert3Result.Metadata.Split.CreatedNode.(*LeafNode)
	assert.Equal(t, uint32(1), newLeaf.GetElementsCount())
	firstKeyInNewLeaf, getKeyInNewLeafErr := newLeaf.GetKeyDataRefByIndex(0)
	assert.NoError(t, getKeyInNewLeafErr)
	assert.Equal(t, key1DataRef, firstKeyInNewLeaf)
	firstKeyInNewLeafData := newLeaf.GetKeyRefData(firstKeyInNewLeaf)
	assert.Equal(t, key1Data, firstKeyInNewLeafData)

	_, getKey2InNewLeafErr := newLeaf.GetKeyDataRefByIndex(1)
	assert.ErrorIs(t, ErrKeyRefAtIndexDoesNotExist, getKey2InNewLeafErr)
}
