package operations

import (
	"bricker-db/btree/node"
	"bricker-db/pager"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestInsertOperation(t *testing.T) {
	tempDir := os.TempDir()
	dbFileName := tempDir + "data.db"
	defer os.Remove(dbFileName)

	pager, pagerErr := pager.NewPager(dbFileName)
	assert.NoError(t, pagerErr)

	initErr := initRootNode(pager, 350)
	assert.NoError(t, initErr)

	key1 := uint32(2)
	key1Data := []byte("key1Data")

	key2 := uint32(0)
	key2Data := []byte("key2Data")

	key3 := uint32(1)
	key3Data := []byte("key3Data")

	key4 := uint32(3)
	key4Data := []byte("key4Data")

	insert1Err := Insert(pager, key1, key1Data)
	assert.NoError(t, insert1Err)

	insert2Err := Insert(pager, key2, key2Data)
	assert.NoError(t, insert2Err)

	insert3Err := Insert(pager, key3, key3Data)
	assert.NoError(t, insert3Err)

	insert4Err := Insert(pager, key4, key4Data)
	assert.NoError(t, insert4Err)

	pagedRoot, readErr := pager.ReadRootNode()
	assert.NoError(t, readErr)

	assert.Equal(t, uint32(2), pagedRoot.Page)
	root, rootOk := pagedRoot.Node.(*node.InternalNode)
	assert.True(t, rootOk)

	assert.Equal(t, uint32(2), root.GetElementsCount())

	keyRef1, keyRef1Err := root.GetKeyPageRefByIndex(0)
	assert.NoError(t, keyRef1Err)
	assert.Equal(t, uint32(2), keyRef1.Key)
	assert.Equal(t, uint32(0), keyRef1.PageId)

	keyRef2, keyRef2Err := root.GetKeyPageRefByIndex(1)
	assert.NoError(t, keyRef2Err)
	assert.Equal(t, uint32(3), keyRef2.Key)
	assert.Equal(t, uint32(1), keyRef2.PageId)
}
