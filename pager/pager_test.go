package pager

import (
	"bricker-db/btree/node"
	"bytes"
	"errors"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func fileExist(path string) bool {
	if _, fileErr := os.OpenFile(path, os.O_RDWR, 0644); errors.Is(fileErr, os.ErrNotExist) {
		return false
	}

	return true
}

func TestPagerCreatesNewDatabaseFile(t *testing.T) {
	tempDir := os.TempDir()
	dbFileName := tempDir + "data.db"
	defer os.Remove(dbFileName)

	assert.False(t, fileExist(dbFileName))

	_, pagerErr := NewPager(dbFileName)
	assert.Nil(t, pagerErr)

	assert.True(t, fileExist(dbFileName))
}

func TestPagerInitedFromExistingFile(t *testing.T) {
	tempDir := os.TempDir()
	dbFileName := tempDir + "data.db"
	defer os.Remove(dbFileName)

	pager1, pager1Err := initPagerInNewFile(dbFileName)
	assert.Nil(t, pager1Err)
	pager1.CloseFile()

	pager2, pager2Err := NewPager(dbFileName)
	assert.Nil(t, pager2Err)

	var expectedHeader [5]byte
	copy(expectedHeader[:], "my db")
	assert.Equal(t, expectedHeader, pager2.header.MagicString)
}

func TestPagerWritePage(t *testing.T) {
	tempDir := os.TempDir()
	dbFileName := tempDir + "data.db"
	defer os.Remove(dbFileName)

	pager, pagerErr := NewPager(dbFileName)
	assert.Nil(t, pagerErr)

	pageData := NewPageBuffer()
	err := pager.WritePage(0, pageData)
	assert.Nil(t, err)

	stat, statErr := pager.file.Stat()
	assert.Nil(t, statErr)

	assert.Equal(t, int64(DATABASE_HEADER_SIZE+PAGE_SIZE), stat.Size())
}

func TestPagerWriteNewPage(t *testing.T) {
	tempDir := os.TempDir()
	dbFileName := tempDir + "data.db"
	defer os.Remove(dbFileName)

	pager, pagerErr := NewPager(dbFileName)
	assert.Nil(t, pagerErr)

	pageData := NewPageBuffer()
	pageId, err := pager.WriteNewPage(pageData)
	assert.Nil(t, err)
	assert.Equal(t, uint32(0), pageId)

	stat, statErr := pager.file.Stat()
	assert.Nil(t, statErr)

	assert.Equal(t, int64(DATABASE_HEADER_SIZE+PAGE_SIZE), stat.Size())
	assert.Equal(t, uint32(1), pager.header.PageCount)
}

func TestPagerReadPage(t *testing.T) {
	tempDir := os.TempDir()
	dbFileName := tempDir + "data.db"
	defer os.Remove(dbFileName)

	pager, pagerErr := NewPager(dbFileName)
	assert.Nil(t, pagerErr)

	// write data
	pageData := bytes.Repeat([]byte("1"), PAGE_SIZE)
	pageId, err := pager.WriteNewPage(pageData)
	assert.Nil(t, err)

	// read data
	data, err := pager.ReadPage(pageId)
	assert.Nil(t, err)

	assert.Equal(t, pageData, data)
}

func TestPagerReadingNonExistingPage(t *testing.T) {
	tempDir := os.TempDir()
	dbFileName := tempDir + "data.db"
	defer os.Remove(dbFileName)

	pager, pagerErr := NewPager(dbFileName)
	assert.Nil(t, pagerErr)

	data, err := pager.ReadPage(1)
	assert.Nil(t, data)
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "failed to read page data")
}

func TestPagerWriteReadPagedNode(t *testing.T) {
	tempDir := os.TempDir()
	dbFileName := tempDir + "data.db"
	defer os.Remove(dbFileName)

	pager, pagerErr := NewPager(dbFileName)
	assert.NoError(t, pagerErr)

	leaf := node.NewEmptyLeafNode(1024)
	key := uint32(0)
	data := []byte("data")
	_, insertErr := leaf.Insert(key, data)
	assert.NoError(t, insertErr)

	pagedNode, writeErr := pager.WriteNewNode(leaf)
	assert.NoError(t, writeErr)

	readPagedNode, readErr := pager.ReadPagedNode(pagedNode.Page)
	assert.NoError(t, readErr)

	readLeafNode := readPagedNode.Node.(*node.LeafNode)
	assert.Equal(t, uint32(1), readLeafNode.GetElementsCount())
	refKey, refKeyErr := readLeafNode.GetKeyDataRefByIndex(0)
	assert.NoError(t, refKeyErr)
	assert.Equal(t, key, refKey.GetKey())

	readData := readLeafNode.GetKeyRefData(refKey)
	assert.Equal(t, data, readData)
}
