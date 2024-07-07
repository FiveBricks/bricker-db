package node

const KEY_DATA_REF_SIZE = 100

type KeyDataReference struct {
	Key    uint32
	Offset uint32
	Length uint32
}

func (k *KeyDataReference) GetKey() uint32 {
	return k.Key
}

type KeyDataReferenceCommit struct {
	keyDataRef *KeyDataReference
	committed  bool
}
