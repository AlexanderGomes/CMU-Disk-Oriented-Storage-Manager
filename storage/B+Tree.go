package storage

const (
	Order      = 3
	Threshold  = Order - 1
	Mid        = Order / 2
	MidPlusOne = Mid + 1
)

type BPlusTree struct {
	Root Node
}

type Key int
type Node struct {
	Keys     []Key
	Children []*Node
	Data     []*LeafRecord
	IsLeaf   bool
	NextLeaf *Node
	Parent   *Node
}

type LeafRecord struct {
	ID     Key
	Values Row
}

func (t *BPlusTree) InsertNode(node *Node) {

}
