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

	if insertResult.Metadata.Split != nil {
		return propagateSplitUpdates(pager, insertResult.Metadata.Split, breadcrumbs)
	}

	return nil
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

func propagateSplitUpdates(pager *pg.Pager, splitMetadata *node.SplitMetadata, breadscrumbs []*Breadcrumb) error {
	// todo: propagate changes
	split := splitMetadata
	breadcrumbsIndex := len(breadscrumbs) - 1
	currentNodeBreadcrumb := getBreadcrumb(breadcrumbsIndex, breadscrumbs)
	parentNodeBreadcrumb := getBreadcrumb(breadcrumbsIndex-1, breadscrumbs)
	breadcrumbsIndex -= 1

	for split != nil {
		// flush the created node
		newPagedNode, writeErr := pager.WriteNewNode(split.CreatedNode)
		if writeErr != nil {
			return writeErr
		}

		maxKey, maxKeyErr := split.CreatedNode.GetMaxKey()
		if maxKeyErr != nil {
			return maxKeyErr
		}

		if parentNodeBreadcrumb == nil {
			// if no parent then we are splitting root
			// create new root node
			newRoot := node.NewEmptyInternalNode(node.INTERNAL_NODE_SIZE)
			newRoot.Insert(split.SplitKey, currentNodeBreadcrumb.pagedNode.Page)
			newRoot.Insert(maxKey, newPagedNode.Page)

			_, writeRootErr := pager.WriteNewRootNode(newRoot)
			return writeRootErr

		}

		parentNode, parentNodeOk := parentNodeBreadcrumb.pagedNode.Node.(*node.InternalNode)
		if !parentNodeOk {
			return errors.New("failed to cast parent node to internal node")
		}

		if !currentNodeBreadcrumb.isRightMostNode {
			// update old key page ref key to created node
			_, updateErr := parentNode.UpdateAtIndex(currentNodeBreadcrumb.index, currentNodeBreadcrumb.key, newPagedNode.Page)
			if updateErr != nil {
				return updateErr
			}

			// add new divider
			insertResult, insertErr := parentNode.Insert(split.SplitKey, currentNodeBreadcrumb.pagedNode.Page)
			if insertErr != nil {
				return insertErr
			}

			split = insertResult.Metadata.Split

		} else {
			// update old key ref key and point to the old node
			_, updateErr := parentNode.UpdateAtIndex(currentNodeBreadcrumb.index, split.SplitKey, currentNodeBreadcrumb.pagedNode.Page)
			if updateErr != nil {
				return updateErr
			}

			// add new divider
			insertResult, insertErr := parentNode.Insert(maxKey, newPagedNode.Page)
			if insertErr != nil {
				return insertErr
			}

			split = insertResult.Metadata.Split
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
