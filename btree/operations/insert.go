package operations

import (
	"bricker-db/btree/node"
	pg "bricker-db/pager"
	"fmt"
)

type Breadcrumb struct {
	pagedNode *pg.PagedNode
	isLeaf    bool
}

func Insert(pager *pg.Pager, key uint32, data []byte) error {
	breadcrumbs, searchErr := findPosition(pager, key)
	if searchErr != nil {
		return searchErr
	}

	leafBreadcrumb, breadcrumbs := breadcrumbs[len(breadcrumbs)-1], breadcrumbs[:len(breadcrumbs)-1]
	leaf, leafOk := leafBreadcrumb.pagedNode.Node.(*node.LeafNode)
	if !leafOk {
		return fmt.Errorf("unable to cast to leaf node")
	}

	insertResult, insertErr := leaf.Insert(key, data)
	if insertErr != nil {
		return insertErr
	}

	if insertResult.Metadata.Split != nil {
		return propagateSplitUpdates(insertResult.Metadata.Split, breadcrumbs)
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
	for {
		breadcrumbs = append(breadcrumbs, &Breadcrumb{currentNode, false})
		if currentNode.GetNodeType() == node.LeafNodeType {
			return breadcrumbs, nil
		} else {
			internalNode, internalNodeOk := currentNode.Node.(*node.InternalNode)
			if !internalNodeOk {
				return nil, fmt.Errorf("failed to cast internal node")
			}

			keyRef, findErr := internalNode.FindPositionForKey(key)
			if findErr != nil {
				return nil, fmt.Errorf("failed to find position for %d: %w", key, findErr)
			}

			pagedNode, readErr := pager.ReadPagedNode(keyRef.PageId)
			if readErr != nil {
				return nil, readErr
			}

			currentNode = pagedNode
		}

	}
}

func propagateSplitUpdates(split *node.SplitMetadata, breadscrumbs []*Breadcrumb) error {
	// todo: propagate changes
	return nil
}
