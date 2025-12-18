package btree

import "bytes"

type BTree struct {
	root uint64
	// Read data from a given page number
	get func(uint64) []byte
	// Create a new page with given data
	create func([]byte) uint64
	// Delete/dealloc the given page number
	del func(uint64)
}

const (
	MERGE_THRESHOLD_BTYES = BTREE_PAGE_SIZE_BYTES / 4
)

func treeInsert(tree *BTree, node BNode, key, val []byte) BNode {
	next := BNode(make([]byte, 2*BTREE_PAGE_SIZE_BYTES))

	index := nodeLookupLE(node, key)
	switch node.btype() {
	case LEAF:
		// TODO: Error handling
		key, _ := node.getKey(index)
		if bytes.Equal(key, key) {
			leafUpdate(next, node, index, key, val)
		} else {
			leafInsert(next, node, index+1, key, val)
		}
	case NODE: // Internal node, walk into the child node
		// TODO: Error handling
		// Recursively insert into child node
		kptr, _ := node.getPtr(index)
		knode := treeInsert(tree, tree.get(kptr), key, val)
		// After we insert, split
		numsplits, splitNodes := nodeSplit3(knode)
		tree.del(kptr)
		nodeReplaceKidN(tree, next, node, index, splitNodes[:numsplits])
	}
	return next
}

/*
Insert key into tree, with the associated val.
Returns the error, if any, encountered during.
*/
func (tree *BTree) Insert(key []byte, val []byte) error {
	if err := checkLimit(key, val); err != nil {
		return err
	}

	// The case where the tree is empty
	if tree.root == 0 {
		root := BNode(make([]byte, BTREE_PAGE_SIZE_BYTES))
		root.setHeader(LEAF, 2)
		// Sentinel value
		nodeAppendKeyVal(root, 0, 0, nil, nil)
		nodeAppendKeyVal(root, 1, 1, nil, nil)
		tree.root = tree.create(root)
		return nil
	}

	// The case where the tree root is not empty.
	node := treeInsert(tree, tree.get(tree.root), key, val)

	// If the root splits as a result of said insert, grow the tree.
	numSplits, splitNodes := nodeSplit3(node)
	tree.del(tree.root)
	if numSplits > 1 {
		root := BNode(make([]byte, BTREE_PAGE_SIZE_BYTES))
		root.setHeader(NODE, numSplits)
		for i, knode := range splitNodes[:numSplits] {
			ptr := tree.create(knode)
			key, _ := knode.getKey(0)
			nodeAppendKeyVal(root, uint16(i), ptr, key, nil)
		}
		tree.root = tree.create(root)
	} else {
		tree.root = tree.create(splitNodes[0])
	}
	return nil
}

/*
Delete key from tree.
Returns:

	Whether the deletion was successful
	Error (if any) encountered
*/
func (tree *BTree) Delete(key []byte) (bool, error)

/*
	Should the updated child node be merged with a sibling node.

Returns:

	An int representing the offset
	The sibling that should be merged with
*/
func shouldMerge(tree *BTree, node BNode, index uint16, updated BNode) (int, BNode) {

	if updated.nbytes() > MERGE_THRESHOLD_BTYES {
		return 0, BNode{}
	}

	if index > 0 {
		ptr, _ := node.getPtr(index - 1)
		sibling := BNode(tree.get(ptr))
		mergedSize := sibling.nbytes() + updated.nbytes() - HEADER_SIZE
		if mergedSize <= BTREE_PAGE_SIZE_BYTES {
			return -1, sibling
		}
	}

	if index+1 < node.nkeys() {
		ptr, _ := node.getPtr(index + 1)
		sibling := BNode(tree.get(ptr))
		mergedSize := sibling.nbytes() + updated.nbytes() - HEADER_SIZE
		if mergedSize <= BTREE_PAGE_SIZE_BYTES {
			return +1, sibling
		}
	}

	return 0, BNode{}
}
