package operations

import (
	"bricker-db/btree/node"
	pg "bricker-db/pager"
)

func Init(pager *pg.Pager) error {
	if pager.RootNodeInitialized() {
		return nil
	}

	newRoot := node.NewEmptyLeafNode(node.LEAF_NODE_SIZE)
	_, writeErr := pager.WriteNewRootNode(newRoot)
	return writeErr
}
