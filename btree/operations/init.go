package operations

import (
	"bricker-db/btree/node"
	pg "bricker-db/pager"
)

func Init(pager *pg.Pager) error {
	if pager.RootNodeInitialized() {
		return nil
	}

	return initRootNode(pager, node.LEAF_NODE_SIZE)
}

func initRootNode(pager *pg.Pager, size uint32) error {
	newRoot := node.NewEmptyLeafNode(size)
	_, writeErr := pager.WriteNewRootNode(newRoot)
	return writeErr
}
