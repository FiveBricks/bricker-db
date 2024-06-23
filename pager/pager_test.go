package pager

import (
	"errors"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
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
