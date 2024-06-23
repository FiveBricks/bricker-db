package pager

import (
	"bricker-db/utils"
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
)

const DATABASE_HEADER_SIZE = 100
const MAGIC_STRING = "my db"

type DatabaseHeader struct {
	MagicString   [len(MAGIC_STRING)]byte
	PageSizeBytes uint32
	PageCount     uint32
	RootPageId    uint32
}

func NewDefaultDatabaseHeader() *DatabaseHeader {
	header := &DatabaseHeader{
		PageSizeBytes: 4096,
		PageCount:     1,
		RootPageId:    0,
	}

	copy(header.MagicString[:], MAGIC_STRING)

	return header
}

func ReadFromFile(file *os.File) (*DatabaseHeader, error) {
	buf := make([]byte, DATABASE_HEADER_SIZE)
	if _, err := file.ReadAt(buf, 0); err != nil {
		return nil, fmt.Errorf("failed to read database header from file: %v", err)
	}

	var header DatabaseHeader
	reader := bytes.NewReader(buf)
	if err := binary.Read(reader, binary.LittleEndian, &header); err != nil {
		return nil, fmt.Errorf("failed to parse header data: %v", err)
	}

	return &header, nil
}

func (h *DatabaseHeader) encode() ([]byte, error) {
	buf := make([]byte, DATABASE_HEADER_SIZE)
	writer := utils.NewFixedSizeSliceWriter(buf)
	if err := binary.Write(writer, binary.LittleEndian, h); err != nil {
		return nil, fmt.Errorf("failed to encode database header: %v", err)
	}

	return buf, nil
}

func (h *DatabaseHeader) WriteToFile(file *os.File) error {
	data, encodeErr := h.encode()
	if encodeErr != nil {
		return encodeErr
	}

	if _, err := file.WriteAt(data, 0); err != nil {
		return fmt.Errorf("failed to write database header into the file: %v", err)
	}

	if err := file.Sync(); err != nil {
		return fmt.Errorf("failed to flush database header into the file: %v", err)
	}

	return nil
}
