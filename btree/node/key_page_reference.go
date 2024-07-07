package node

const KEY_PAGE_REF_SIZE = 100

type KeyPageReference struct {
	Key    uint32
	PageId uint32
}

func (k *KeyPageReference) GetKey() uint32 {
	return k.Key
}

type KeyPageReferenceCommit struct {
	keyPageRef *KeyPageReference
	committed  bool
}
