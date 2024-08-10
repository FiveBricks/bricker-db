package operations

import (
	"bricker-db/pager"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestInitOperationCreatesRootNode(t *testing.T) {
	tempDir := os.TempDir()
	dbFileName := tempDir + "data.db"
	defer os.Remove(dbFileName)

	pager, pagerErr := pager.NewPager(dbFileName)
	assert.NoError(t, pagerErr)

	assert.False(t, pager.RootNodeInitialized())
	_, readUninitializedRootErr := pager.ReadRootNode()
	assert.Error(t, readUninitializedRootErr)

	initErr := Init(pager)
	assert.NoError(t, initErr)

	rootNode, readRootErr := pager.ReadRootNode()
	assert.NoError(t, readRootErr)
	assert.Equal(t, uint32(0), rootNode.Page)
}
