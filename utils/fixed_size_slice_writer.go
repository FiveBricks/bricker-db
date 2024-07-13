package utils

import (
	"fmt"
)

type FixedSizeSliceWriter struct {
	buf    []byte
	offset int
}

func NewFixedSizeSliceWriter(buf []byte) *FixedSizeSliceWriter {
	return &FixedSizeSliceWriter{buf, 0}
}

func (f *FixedSizeSliceWriter) Write(p []byte) (n int, err error) {
	if len(p) > cap(f.buf) {
		return 0, fmt.Errorf("buffer is to small: buffer capacity: %d, len of data being written: %d", cap(f.buf), len(p))
	}

	bytesWritten := copy(f.buf[f.offset:], p)
	f.offset += bytesWritten

	return bytesWritten, nil
}
