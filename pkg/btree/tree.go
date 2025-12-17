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
