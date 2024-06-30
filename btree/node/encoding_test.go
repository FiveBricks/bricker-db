package node

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestKeyDataRefEncodingDecoding(t *testing.T) {
	key := &KeyDataReference{
		1,
		2,
		3,
	}

	data, encodingErr := EncodeKeyDataRef(key)
	assert.NoError(t, encodingErr)
	assert.Equal(t, KEY_DATA_REF_SIZE, len(data))

	decodedKey, decodingErr := DecodeKeyDataRef(data)
	assert.NoError(t, decodingErr)
	assert.Equal(t, key, decodedKey)
}
