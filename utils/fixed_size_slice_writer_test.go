package utils

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func checkSliceIsZerosAfterIndex(index int, b []byte) bool {
	for i := index; i < len(b); i++ {
		if b[i] != 0 {
			return false
		}
	}

	return true
}

func TestFixedSizeSliceWriterShouldWriteData(t *testing.T) {
	text := "asd"
	dataToWrite := []byte(text)
	buf := make([]byte, 10)
	writer := NewFixedSizeSliceWriter(buf)
	bytesWritten, writerErr := writer.Write(dataToWrite)

	assert.Nil(t, writerErr)
	assert.Equal(t, len(text), bytesWritten)
	assert.Equal(t, buf[:len(text)], []byte(text))
	assert.True(t, checkSliceIsZerosAfterIndex(len(text), buf))
}

func TestFixedSizeSliceWriterOverwritesData(t *testing.T) {
	data := []byte("asd")
	buf := make([]byte, 10)
	writer := NewFixedSizeSliceWriter(buf)
	writer.Write(data)

	// overwrite buffer with new data
	newText := "xyz123"
	newData := []byte(newText)
	writer.Write(newData)

	assert.Equal(t, buf[:len(newText)], []byte(newText))
	assert.True(t, checkSliceIsZerosAfterIndex(len(newText), buf))
}

func TestFixedSizeSliceWriterErrorsWhenTooMuchDataIsWritten(t *testing.T) {
	text := "asd"
	dataToWrite := []byte(text)
	buf := make([]byte, 1)
	writer := NewFixedSizeSliceWriter(buf)
	bytesWritten, writerErr := writer.Write(dataToWrite)
	assert.Zero(t, bytesWritten)
	assert.NotNil(t, writerErr)
	assert.Equal(t, "buffer is to small: buffer capacity: 1, len of data being written: 3", writerErr.Error())
}
