package utils

import (
	"fmt"
)

type FixedSizeSliceWriter struct {
	buf []byte
}

func NewFixedSizeSliceWriter(buf []byte) *FixedSizeSliceWriter {
	return &FixedSizeSliceWriter{buf}
}

func (f *FixedSizeSliceWriter) Write(p []byte) (n int, err error) {
	if len(p) > cap(f.buf) {
		return 0, fmt.Errorf("buffer is to small: buffer capacity: %d, len of data being written: %d", cap(f.buf), len(p))
	}
	return copy(f.buf, p), nil
}
