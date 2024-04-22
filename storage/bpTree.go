package storage

// not being used right now
type DirectoryPageTree struct {
	tree BPlusTree
}

type BPlusTree struct {
	root *node
}

type node struct {
	isLeaf   bool
	keys     []PageID
	offsets  []Offset
	children []*node
}

func NewBPlusTree() *BPlusTree {
	return &BPlusTree{}
}

const maxKeys = 5

func (bpt *BPlusTree) Insert(key PageID, value Offset) {
	if bpt.root == nil {
		bpt.root = &node{isLeaf: true, keys: []PageID{key}, offsets: []Offset{value}}
		return
	}
	splitKey, splitNode := bpt.root.insert(key, value)
	if splitNode != nil {
		newRoot := &node{
			isLeaf:   false,
			keys:     []PageID{splitKey},
			children: []*node{bpt.root, splitNode},
		}
		bpt.root = newRoot
	}
}

func (n *node) insert(key PageID, offset Offset) (PageID, *node) {
	if n.isLeaf {
		n.insertLeaf(key, offset)
		if len(n.keys) > maxKeys {
			return n.split()
		}
		return 0, nil
	}
	index := n.findIndex(key)
	splitKey, splitNode := n.children[index].insert(key, offset)
	if splitNode != nil {
		n.insertChild(splitKey, splitNode, index+1)
		if len(n.keys) > maxKeys {
			return n.split()
		}
		return 0, nil
	}
	return 0, nil
}

func (n *node) insertLeaf(key PageID, offset Offset) {
	index := n.findIndex(key)
	n.keys = append(n.keys[:index], append([]PageID{key}, n.keys[index:]...)...)
	n.offsets = append(n.offsets[:index], append([]Offset{offset}, n.offsets[index:]...)...)
}

func (n *node) findIndex(key PageID) int {
	for i, k := range n.keys {
		if key < k {
			return i
		}
	}
	return len(n.keys)
}

func (n *node) insertChild(key PageID, child *node, index int) {
	n.keys = append(n.keys[:index], append([]PageID{key}, n.keys[index:]...)...)
	n.children = append(n.children[:index+1], append([]*node{child}, n.children[index+1:]...)...)
}

func (n *node) split() (PageID, *node) {
	mid := len(n.keys) / 2
	splitKey := n.keys[mid]
	splitNode := &node{
		isLeaf:  n.isLeaf,
		keys:    append([]PageID{}, n.keys[mid+1:]...),
		offsets: append([]Offset{}, n.offsets[mid+1:]...),
	}
	if !n.isLeaf {
		splitNode.children = append([]*node{}, n.children[mid+1:]...)
		n.children = n.children[:mid+1]
	}
	n.keys = n.keys[:mid]
	n.offsets = n.offsets[:mid]
	return splitKey, splitNode
}

func (bpt *BPlusTree) Search(key PageID) (Offset, bool) {
	if bpt.root == nil {
		return 0, false
	}
	return bpt.root.search(key)
}

func (n *node) search(key PageID) (Offset, bool) {
	if n.isLeaf {
		index := n.findIndex(key)
		if index < len(n.keys) && n.keys[index] == key {
			return n.offsets[index], true
		}
		return 0, false
	}
	index := n.findIndex(key)
	return n.children[index].search(key)
}
