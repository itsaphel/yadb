// This file implements B+ Trees for use in a database system.
//
// All values are stored solely on the leaf level. All operations (like inserting,
// updating, removing and retrieval) affect only leaf nodes. They propagate to
// higher levels only during splits and merges.
//
// In a B+ Tree, we access at most log_C(N) pages (where N is number of tuples
// and C is node capacity). The number of comparisons is log_2(N)
// TODOs:
// * How do we record the key range supported by a node?

package yadb

import (
	"sort"
	"strings"
)

type Tree struct {
	root   *Node
	degree int
}

// NewTree creates a new B-Tree with the given degree
func NewTree(degree int) *Tree {
	if degree < 2 {
		panic("Degree must be >= 2")
	}

	tree := &Tree{
		degree: degree,
	}
	tree.root = tree.NewEmptyNode(true)
	return tree
}

// Get Returns a pointer to the KeyValuePair if the key exists in this Tree
// otherwise returns nil
func (tree *Tree) Get(key string) *KeyValuePair {
	if tree.root == nil {
		return nil
	}
	return tree.root.get(key)
}

// Insert a key-value pair into the tree. The pair will be inserted at the bottom
// of the tree, and changes propagate up to internal nodes if required for splits/merges
// If an existing value for the key exists, Insert will overwrite the existing value
func (tree *Tree) Insert(key string, value string) {
	// Locate the appropriate leaf to insert into
	leaf := tree.root.findLeafNodeForKey(key)
	kvPair := &KeyValuePair{
		key:   key,
		value: value,
	}

	leaf.insert(kvPair)
}

// Delete removes a key from the tree
func (tree *Tree) Delete(key string) {
	leaf := tree.root.findLeafNodeForKey(key)
	leaf.delete(key)
}

// Node represents an internal node in a B+ Tree.
// Each internal node holds up to N keys and N+1 pointers to child nodes.
// Each leaf node holds N pointers to heap data
//
// The occupancy of the tree is the number of children as a fraction of the total
// capacity. We wish to keep a high occupancy. Splits and merges will be triggered
// to maintain this property.
//
// children are stored in sorted order to allow for binary searches.
type Node struct {
	tree   *Tree
	parent *Node // Retain parent for rebalancing / splitting operations

	keys     []string
	pointers []interface{}

	isLeaf bool
}

func (tree *Tree) NewEmptyNode(isLeaf bool) *Node {
	//return &Node{
	//	tree:     tree,
	//	children: make(nodes, 0),
	//	tuples:   make(tuples, 0),
	//	isLeaf:   isLeaf,
	//	parent:   nil,
	//}

	return &Node{
		tree:     tree,
		parent:   nil,
		keys:     make([]string, 0), // maybe size shouldn't be 0 - resizing time
		pointers: make([]interface{}, 0),
		isLeaf:   isLeaf,
	}
}

type KeyValuePair struct {
	key   string
	value string
}

// get returns a pointer to the KV Pair if the key exists under this node (or
// its children, recursively), otherwise returns nil. In either case, the
// leaf node is also returned.
func (n *Node) get(key string) *KeyValuePair {
	leaf := n.findLeafNodeForKey(key)

	// try to find the specific KV pair in the leaf node's tuples
	pair, _ := leaf.findKeyInLeaf(key)
	return pair
}

// findIndex returns the appropriate pointer to follow to find a key in a
// B+ tree, and whether there was an exact match
func (n *Node) findIndex(key string) (int, bool) {
	// sort.Search does a binary search and returns the lowest index s.t. ret != -1
	var exact bool
	i := sort.Search(len(n.keys), func(i int) bool {
		ret := strings.Compare(key, n.keys[i])
		if ret == 0 {
			exact = true
		}
		return ret != 0
	})

	if !exact && len(n.keys) > 1 {
		i--
	}

	return i, exact
}

// findLeafNodeForKey finds the leaf node that should contain the key
func (n *Node) findLeafNodeForKey(key string) *Node {
	if n.isLeaf {
		return n
	}

	// Find the child internal Node under which the key would lie, if it were
	// in the tree rooted by the current node
	i, _ := n.findIndex(key)
	return n.pointers[i].(*Node).findLeafNodeForKey(key)
}

// findKeyInLeaf searches a leaf node for the presence of a key.
// If found, returns KV-pair and corresponding index
// Otherwise returns nil and the expected index of its pointers
func (n *Node) findKeyInLeaf(key string) (*KeyValuePair, int) {
	var exact bool
	i := sort.Search(len(n.pointers), func(i int) bool {
		ret := strings.Compare(key, n.pointers[i].(*KeyValuePair).key)
		if ret == 0 {
			exact = true
		}
		return ret != -1
	})

	if exact {
		return n.pointers[i].(*KeyValuePair), i
	} else {
		return nil, i
	}

	// Alt implementation:
	//for i := 0; i < len(n.pointers); i++ {
	//	kv := n.pointers[i].(*KeyValuePair)
	//	if kv.key == key {
	//		return kv, i
	//	} else if kv.key > key {
	//		return nil, i
	//	}
	//}
	//
	//return nil, len(n.pointers)
}

// insert a key-value pair into a leaf node.
func (n *Node) insert(pair *KeyValuePair) {
	if !n.isLeaf {
		panic("Tried to insert KV-Pair to non-leaf node")
	}

	// Find insertion index
	existing, index := n.findKeyInLeaf(pair.key)
	if existing != nil {
		// If key is already present, overwrite existing value
		existing.value = pair.value
	} else {
		// Otherwise, allocate space for a new KV pair
		n.keys = append(n.keys, "")
		copy(n.keys[index+1:], n.keys[index:])
		n.keys[index] = pair.key

		n.pointers = append(n.pointers, &KeyValuePair{})
		copy(n.pointers[index+1:], n.pointers[index:])
		n.pointers[index] = pair
	}

	n.split()
}

// delete removes a key from a leaf node
func (n *Node) delete(key string) {
	kv, i := n.findKeyInLeaf(key)
	// if kv == nil, the key could not be found in the tree
	// else
	if kv != nil {
		// remove the KV pair from the leaf's tuples
		n.keys = append(n.keys[:i], n.keys[i+1:]...)
		n.pointers = append(n.pointers[:i], n.pointers[i+1:]...)
	}

	n.maybeMerge()
}

// Node maintenance operations

// truncate functions remove items after index i, in a manner that prevents memleaks
// see https://utcc.utoronto.ca/~cks/space/blog/programming/GoSlicesMemoryLeak

func (n *Node) truncateKeys(index int) {
	n.keys = n.keys[:index+1]
}

func (n *Node) truncatePointers(index int) {
	for i := index + 1; i < len(n.pointers); i++ {
		n.pointers[i] = nil
	}
	n.pointers = n.pointers[:index+1]
}

// splitLeaf splits a leaf node into two, if needed
func (n *Node) split() {
	if len(n.pointers) <= n.tree.degree {
		return
	}

	// If this is the root, we need to create a new root
	if n.parent == nil {
		n.parent = n.tree.NewEmptyNode(false)
		n.parent.pointers = append(n.parent.pointers, n)
		n.tree.root = n.parent
	}

	// Allocate a new node, transfer half of the tuples in this node to the
	// new node. Set the parent of this new node as the original node's parent.
	// Splitting is a recursive operation; the parents may also need to be split.

	// Pointers from index N/2 + 1 are moved to new node
	splitIndex := n.tree.degree / 2
	next := n.tree.NewEmptyNode(true)
	next.keys = append(next.keys, n.keys[splitIndex+1:]...)
	next.pointers = append(next.pointers, n.pointers[splitIndex+1:]...)
	n.truncateKeys(splitIndex)
	n.truncatePointers(splitIndex)

	// Add first key and pointer to parent node
	n.parent.keys = append(n.parent.keys, next.keys[0]) // TODO this is prob not correct location
	n.parent.pointers = append(n.parent.pointers, next)

	if n.parent != nil {
		n.parent.split()
	}
}

// TODO impl
func (n *Node) maybeMerge() {

}
