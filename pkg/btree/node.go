package btree

import (
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

type Node struct {
	keys     [][]byte
	vals     [][]byte
	children []*Node
}

// For decoding nodes real fast
type BNode []byte

func (node BNode) btype() uint16 {
	return binary.LittleEndian.Uint16(node[0:2])
}

func (node BNode) nkeys() uint16 {
	return binary.LittleEndian.Uint16(node[2:4])
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

func encode(node *Node) []byte {
	return []byte{}
}

func decode(page []byte) *Node {
	return &Node{}
}
