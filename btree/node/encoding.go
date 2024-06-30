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
		return nil, fmt.Errorf("failed to encode leaf node: %v", err)
	}

	return buf, nil
}

func DecodeKeyDataRef(data []byte) (*KeyDataReference, error) {
	keyDataRef := &KeyDataReference{}
	buf := bytes.NewReader(data)
	if err := binary.Read(buf, binary.LittleEndian, keyDataRef); err != nil {
		return nil, fmt.Errorf("failed to decode leaf node: %v", err)
	}

	return keyDataRef, nil
}
