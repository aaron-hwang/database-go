package btree

import (
	"bytes"
	"encoding/binary"
	"errors"
)

const (
	NODE = 1
	LEAF = 2

	BTREE_PAGE_SIZE_BYTES    = 4096
	BTREE_MAX_KEY_SIZE_BYTES = 1000
	BREE_MAX_VAL_SIZE_BYTES  = 3000

	// More of a reminder than anything else
	HEADER_SIZE = 4
)

// Pseudocode for a btree. However, writing to a direct bytes slice is much faster.
// type Node struct {
// 	// Every node has these.
// 	keys     [][]byte
// 	// Only leaf nodes should have these
// 	vals     [][]byte
// 	// Only internal nodes should have these (non leaf nodes)
// 	children []*Node
// }

// For decoding nodes real fast
type BNode []byte

// Return the
func (node BNode) btype() uint16 {
	return binary.LittleEndian.Uint16(node[0:2])
}

func (node BNode) nkeys() uint16 {
	return binary.LittleEndian.Uint16(node[2:4])
}

func (node BNode) nbytes() uint16 {
	kvpos, _ := node.kvPos(node.nkeys())
	return kvpos
}

func (node BNode) setHeader(btype uint16, nkeys uint16) {
	binary.LittleEndian.PutUint16(node[0:2], btype)
	binary.LittleEndian.PutUint16(node[2:4], nkeys)
}

func (node BNode) getPtr(index uint16) (uint64, error) {
	if !node.isValidIndex(index) {
		return 0, errors.New("out of range index")
	}
	pos := HEADER_SIZE + 8*index
	return binary.LittleEndian.Uint64(node[pos:]), nil
}

func (node BNode) setPtr(index uint16, val uint64) error {
	if !node.isValidIndex(index) {
		return errors.New("out of range index")
	}
	pos := HEADER_SIZE + 8*index
	binary.LittleEndian.PutUint64(node[pos:], val)
	return nil
}

// Get the offsets of the 'index'th kv pair
func (node BNode) getOffset(index uint16) uint16 {
	if index == 0 {
		return 0
	}

	pos := HEADER_SIZE + 8*node.nkeys() + 2*(index-1)
	return binary.LittleEndian.Uint16(node[pos:])
}

// Return the raw position of the 'index'th key
func (node BNode) kvPos(index uint16) (uint16, error) {
	nkeys := node.nkeys()
	if !(index <= nkeys) {
		return 0, errors.New("invalid index")
	}

	return HEADER_SIZE + 8*nkeys + 2*nkeys + node.getOffset(index), nil
}

// Get a value located at index, as a slice. Reminder that slices are references.
func (node BNode) getValue(index uint16) ([]byte, error) {
	if index >= node.nkeys() {
		return nil, errors.New("index out of bounds when accessing key")
	}

	pos, err := node.kvPos(index)
	if err != nil {
		return nil, err
	}

	keylength := binary.LittleEndian.Uint16(node[pos:])
	vallen := binary.LittleEndian.Uint16(node[pos+2:])
	return node[pos+4+keylength:][:vallen], nil

}

// Grabs the key located at index
func (node BNode) getKey(index uint16) ([]byte, error) {
	if index >= node.nkeys() {
		return nil, errors.New("index out of bounds when accessing key")
	}

	pos, err := node.kvPos(index)
	if err != nil {
		return nil, err
	}

	keylen := binary.LittleEndian.Uint16(node[pos:])
	return node[pos+4:][:keylen], nil
}

// Helper function for verifying an index when accessing a node
func (node BNode) isValidIndex(index uint16) bool {
	return index < node.nkeys()
}

// Insert a new key at position index
func leafInsert(next BNode, old BNode, index uint16, key []byte, val []byte) {
	next.setHeader(LEAF, old.nkeys()+1)
	nodeAppendAcrossRange(next, old, 0, 0, index)
}

// Append n keys to next from old,
func nodeAppendAcrossRange(next BNode, old BNode, dest uint16, src uint16, n uint16) {
	for i := uint16(0); i < n; i++ {
		dst, source := dest+i, src+i
		// Placeholder; not sure what to do with error for now; probably propagate?
		oldPtr, _ := old.getPtr(source)
		oldKey, _ := old.getKey(source)
		oldVal, _ := old.getValue(source)
		nodeAppendKeyVal(next, dst, oldPtr, oldKey, oldVal)
	}
}

func nodeAppendKeyVal(next BNode, destination uint16, ptr uint64, key []byte, val []byte) {

}

// Update the given new leaf to
func leafUpdate(next, old BNode, index uint16, key, val []byte) {
	next.setHeader(LEAF, old.nkeys())
	nodeAppendAcrossRange(next, old, 0, 0, index)
	nodeAppendKeyVal(next, index, 0, key, val)
	nodeAppendAcrossRange(next, old, index+1, index+1, old.nkeys()-(index+1))
}

// Find the last position less than or equal to the given key; used to maintain sorted order when updating keys
func nodeLookupLE(node BNode, key []byte) uint16 {
	nkeys := node.nkeys()
	var i uint16
	// TODO: Change to binary search eventually. Probably not a huge issue considering would need thousand+ keys to make diff
	for i = 0; i < nkeys; i++ {
		// For now just discard any errors, future me problem
		compkey, _ := node.getKey(i)
		cmp := bytes.Compare(compkey, key)
		// Equal
		if cmp == 0 {
			return i
		}
		// key is bigger than i
		if cmp > 0 {
			return i - 1
		}
	}

	// We iterated through every position, means key is greater than every other key
	return i - 1
}

// Split a node's keys in half. For writing to disk, make sure a node still fits in one page. Split them among left and right respectively.
func nodeSplitInHalf(left, right, old BNode) error {
	nkeys := old.nkeys()
	if nkeys < 2 {
		return errors.New("placeholder error for when splitting a node in half; number of keys in old less than 2")
	}

	// Try to slice the keys in half, stuff it into a page size
	// If we exceed page size, keep shrinking until we don't
	numleft := nkeys / 2
	left_bytes := func() uint16 {
		return 4 + 8*numleft + 2*numleft + old.getOffset(numleft)
	}

	for left_bytes() > BTREE_PAGE_SIZE_BYTES {
		numleft--
	}

	if numleft < 1 {
		// Placeholderish
		return errors.New("error: number of keys on the left after split attempt was 0.")
	}

	// Do the same for the right. Start from where numleft left off.
	right_bytes := func() uint16 {
		return old.nbytes() - left_bytes()*4
	}

	for right_bytes() > BTREE_PAGE_SIZE_BYTES {
		numleft++
	}

	if numleft >= nkeys {
		return errors.New("error when shifting keys")
	}

	numRight := nkeys - numleft

	left.setHeader(old.btype(), numleft)
	right.setHeader(old.btype(), numRight)
	nodeAppendAcrossRange(left, old, 0, 0, numleft)
	nodeAppendAcrossRange(right, old, 0, 0, numRight)

	return nil
}
