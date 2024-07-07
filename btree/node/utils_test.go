package node

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFindPositionForKey(t *testing.T) {
	leaf := NewEmptyLeafNode(1024)
	data := []byte("data")
	key := uint32(1)

	_, insertErr := leaf.Insert(key, data)
	assert.NoError(t, insertErr)

	key2 := uint32(0)
	exists, index, err := FindPositionForKey(leaf, key2)
	assert.NoError(t, err)
	assert.False(t, exists)
	assert.Equal(t, uint32(0), index)
}
