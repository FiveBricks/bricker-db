package node

const KEY_DATA_REF_SIZE = 100

type KeyDataReference struct {
	Key    uint32
	Offset uint32
	Length uint32
}
