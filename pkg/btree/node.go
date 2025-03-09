package btree

const (
	NODE = 1
	LEAF = 2
)

type Node struct {
	keys     [][]byte
	vals     [][]byte
	children []*Node
}

func encode(node *Node) []byte {
	return []byte{}
}

func decode(page []byte) *Node {
	return &Node{}
}
