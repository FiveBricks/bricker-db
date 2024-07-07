package node

import (
	"bricker-db/utils"
	"bytes"
	"encoding/binary"
	"fmt"
)

func EncodeKeyDataRef(key *KeyDataReference) ([]byte, error) {
	buf := make([]byte, KEY_DATA_REF_SIZE)
	writer := utils.NewFixedSizeSliceWriter(buf)
	if err := binary.Write(writer, binary.LittleEndian, key); err != nil {
		return nil, fmt.Errorf("failed to encode key data ref: %w", err)
	}

	return buf, nil
}

func EncodeKeyPageRef(key *KeyPageReference) ([]byte, error) {
	buf := make([]byte, KEY_PAGE_REF_SIZE)
	writer := utils.NewFixedSizeSliceWriter(buf)
	if err := binary.Write(writer, binary.LittleEndian, key); err != nil {
		return nil, fmt.Errorf("failed to encode key page ref: %w", err)
	}

	return buf, nil
}

func DecodeKeyDataRef(data []byte) (*KeyDataReference, error) {
	keyDataRef := &KeyDataReference{}
	buf := bytes.NewReader(data)
	if err := binary.Read(buf, binary.LittleEndian, keyDataRef); err != nil {
		return nil, fmt.Errorf("failed to decode key data ref: %w", err)
	}

	return keyDataRef, nil
}

func DecodeKeyPageRef(data []byte) (*KeyPageReference, error) {
	keyPageRef := &KeyPageReference{}
	buf := bytes.NewReader(data)
	if err := binary.Read(buf, binary.LittleEndian, keyPageRef); err != nil {
		return nil, fmt.Errorf("failed to decode key page ref: %w", err)
	}

	return keyPageRef, nil
}
