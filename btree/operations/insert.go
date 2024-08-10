package operations

import (
	"bricker-db/btree/node"
	pg "bricker-db/pager"
	"errors"
	"fmt"
)

type Breadcrumb struct {
	pagedNode       *pg.PagedNode
	index           uint32
	key             uint32
	isRightMostNode bool
}

func Insert(pager *pg.Pager, key uint32, data []byte) error {
	breadcrumbs, searchErr := findPosition(pager, key)
	if searchErr != nil {
		return searchErr
	}

	// leafBreadcrumb, breadcrumbs := breadcrumbs[len(breadcrumbs)-1], breadcrumbs[:len(breadcrumbs)-1]
	leafBreadcrumb := breadcrumbs[len(breadcrumbs)-1]
	leaf, leafOk := leafBreadcrumb.pagedNode.Node.(*node.LeafNode)
	if !leafOk {
		return fmt.Errorf("unable to cast to leaf node")
	}

	insertResult, insertErr := leaf.Insert(key, data)
	if insertErr != nil {
		return insertErr
	}

	if writeErr := pager.WritePagedNode(leafBreadcrumb.pagedNode); writeErr != nil {
		return writeErr
	}

	return propagateInsertUpdates(pager, insertResult.Metadata, breadcrumbs)
}

func findPosition(pager *pg.Pager, key uint32) ([]*Breadcrumb, error) {
	rootPagedNode, rootNodeErr := pager.ReadRootNode()
	if rootNodeErr != nil {
		return nil, fmt.Errorf("failed to read root node: %w", rootNodeErr)
	}

	var currentNode *pg.PagedNode
	currentNode = rootPagedNode
	var breadcrumbs []*Breadcrumb
	breadcrumbs = append(breadcrumbs, &Breadcrumb{currentNode, 0, 0, true})
	for {
		if currentNode.GetNodeType() == node.LeafNodeType {
			return breadcrumbs, nil
		} else {
			internalNode, internalNodeOk := currentNode.Node.(*node.InternalNode)
			if !internalNodeOk {
				return nil, fmt.Errorf("failed to cast internal node")
			}

			index, keyRef, findErr := internalNode.FindPositionForKey(key)
			if findErr != nil {
				return nil, fmt.Errorf("failed to find position for %d: %w", key, findErr)
			}

			pagedNode, readErr := pager.ReadPagedNode(keyRef.PageId)
			if readErr != nil {
				return nil, readErr
			}

			isRightMostNode := index == currentNode.Node.GetElementsCount()-1
			currentNode = pagedNode
			breadcrumbs = append(breadcrumbs, &Breadcrumb{currentNode, index, keyRef.GetKey(), isRightMostNode})
		}
	}
}

func handleSplit(pager *pg.Pager, split *node.SplitMetadata, currentNodeBreadcrumb *Breadcrumb, parentNodeBreadcrumb *Breadcrumb) (*node.InsertMetadata, error) {
	// flush the created node
	newPagedNode, writeErr := pager.WriteNewNode(split.CreatedNode)
	if writeErr != nil {
		return nil, writeErr
	}

	maxKey, maxKeyErr := split.CreatedNode.GetMaxKey()
	if maxKeyErr != nil {
		return nil, maxKeyErr
	}

	if parentNodeBreadcrumb == nil {
		// if no parent then we are splitting root
		// create new root node
		newRoot := node.NewEmptyInternalNode(node.INTERNAL_NODE_SIZE)
		newRoot.Insert(split.SplitKey, currentNodeBreadcrumb.pagedNode.Page)
		newRoot.Insert(maxKey, newPagedNode.Page)

		_, writeRootErr := pager.WriteNewRootNode(newRoot)
		return nil, writeRootErr

	}

	parentNode, parentNodeOk := parentNodeBreadcrumb.pagedNode.Node.(*node.InternalNode)
	if !parentNodeOk {
		return nil, errors.New("failed to cast parent node to internal node")
	}

	var insertResult *node.InternalNodeInsertResult
	var insertErr error

	if !currentNodeBreadcrumb.isRightMostNode {
		// update old key page ref key to created node
		_, updateErr := parentNode.UpdateAtIndex(currentNodeBreadcrumb.index, currentNodeBreadcrumb.key, newPagedNode.Page)
		if updateErr != nil {
			return nil, updateErr
		}

		// add new divider
		insertResult, insertErr = parentNode.Insert(split.SplitKey, currentNodeBreadcrumb.pagedNode.Page)
	} else {
		// update old key ref key and point to the old node
		_, updateErr := parentNode.UpdateAtIndex(currentNodeBreadcrumb.index, split.SplitKey, currentNodeBreadcrumb.pagedNode.Page)
		if updateErr != nil {
			return nil, updateErr
		}

		// add new divider
		insertResult, insertErr = parentNode.Insert(maxKey, newPagedNode.Page)
	}

	if insertErr != nil {
		return nil, insertErr
	}

	// persist changes
	if writeErr := pager.WritePagedNode(parentNodeBreadcrumb.pagedNode); writeErr != nil {
		return nil, writeErr
	}
	return insertResult.Metadata, nil
}

func handleNewHighKeyInserted(pager *pg.Pager, update *node.HighKeyUpdate, currentNodeBreadcrumb *Breadcrumb, parentNodeBreadcrumb *Breadcrumb) (*node.InsertMetadata, error) {
	// we only need to propagate changes for right most nodes
	if !currentNodeBreadcrumb.isRightMostNode {
		return nil, nil
	}

	parentNode, parentNodeOk := parentNodeBreadcrumb.pagedNode.Node.(*node.InternalNode)
	if !parentNodeOk {
		return nil, errors.New("failed to cast parent node to internal node")
	}

	parentHighKeyUpdate, updateErr := parentNode.UpdateAtIndex(currentNodeBreadcrumb.index, update.NewHighKey, currentNodeBreadcrumb.pagedNode.Page)
	if updateErr != nil {
		return nil, updateErr
	}

	// persist changes
	if writeErr := pager.WritePagedNode(parentNodeBreadcrumb.pagedNode); writeErr != nil {
		return nil, writeErr
	}

	return &node.InsertMetadata{Split: nil, HighKey: parentHighKeyUpdate}, nil
}

func propagateInsertUpdates(pager *pg.Pager, metadata *node.InsertMetadata, breadscrumbs []*Breadcrumb) error {
	insertMetadata := metadata
	breadcrumbsIndex := len(breadscrumbs) - 1

	for true {
		currentNodeBreadcrumb := getBreadcrumb(breadcrumbsIndex, breadscrumbs)
		parentNodeBreadcrumb := getBreadcrumb(breadcrumbsIndex-1, breadscrumbs)
		breadcrumbsIndex -= 1

		if insertMetadata.Split != nil {
			var splitErr error
			insertMetadata, splitErr = handleSplit(pager, insertMetadata.Split, currentNodeBreadcrumb, parentNodeBreadcrumb)
			if splitErr != nil {
				return splitErr
			}
		} else if insertMetadata.HighKey != nil {
			var highKeyUpdateErr error
			insertMetadata, highKeyUpdateErr = handleNewHighKeyInserted(pager, insertMetadata.HighKey, currentNodeBreadcrumb, parentNodeBreadcrumb)
			if highKeyUpdateErr != nil {
				return highKeyUpdateErr
			}
		} else {
			// done
			return nil
		}

	}

	return nil
}

func getBreadcrumb(index int, breadscrumbs []*Breadcrumb) *Breadcrumb {
	if index < len(breadscrumbs) && index >= 0 {
		return breadscrumbs[index]
	}

	return nil
}
