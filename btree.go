// This file implements B+ Trees for use in a database system.
//
// All values are stored solely on the leaf level. All operations (like inserting,
// updating, removing and retrieval) affect only leaf nodes. They propagate to
// higher levels only during splits and merges.

// TODOs:
// * How do we record the key range supported by a node?

package yadb

import (
	"sort"
	"strings"
)

type Tree struct {
	root *Node
}

func NewTree() *Tree {
	return &Tree{
		root: NewEmptyNode(),
	}
}

// Insert a key-value pair into the tree. The pair will be inserted into the
// bottom, and will propagate up to internal nodes if required for splits/merges
func (n *Tree) Insert(key string, value string) {

}

// Delete removes a key from the tree
func (n *Tree) Delete(key string) {

}

// Node represents an internal node in a B+ Tree.
// Each internal (non-leaf) node holds up to N keys and N+1 pointers to child nodes.
//
// The occupancy of the tree is the relationship between the maximum and actual
// capacity. We wish to keep a high occupancy. Splits and merges will be
// triggered to maintain this property.
//
// children are stored in sorted order to allow for binary searches.
type Node struct {
	key string // TODO rethink this...
	//parent   *Node // we might need this ptr for rebalancing
	children nodes

	isLeaf bool

	// This is defined only for leaf pages. Must be nil for internal nodes.
	tuples tuples
}

func NewEmptyNode() *Node {
	return &Node{
		key:      "",
		children: make(nodes, 0),
		isLeaf:   true,
	}
}

type KeyValuePair struct {
	key   string
	value string
}

// get Returns a pointer to the heap Node if the key exists in this Tree
// (or its children, recursively), otherwise returns nil
func (n *Node) get(key string) *KeyValuePair {
	// If we're on a leaf node, try to findInternal the specific KV-Pair in our items
	if n.isLeaf {
		pair, _ := n.findLeaf(key)
		return pair
	}

	// Otherwise, findInternal the child internal Node under which the key would
	// lie, if it were in the Tree
	i := n.findInternal(key)
	return n.children[i].get(key)
}

// findInternal returns the smallest index i such that key < n.children[i].key
func (n *Node) findInternal(key string) int {
	if n.isLeaf {
		panic("Tried to run findInternal on a leaf node!")
	}

	// sort.Search returns the lowest index s.t. cmp() != -1, but we need
	// the highest index s.t. (cmp() == 1 OR cmp() == 0). Therefore we need to
	// do i-- if it's an inexact match
	var exact bool

	i := sort.Search(len(n.children), func(i int) bool {
		cmp := strings.Compare(n.children[i].key, key)
		if cmp == 0 {
			exact = true
		}
		return cmp != -1
	})

	if !exact && i > 0 {
		i--
	}

	return i
}

// findLeaf searches a leaf node for the presence of a key.
// If found, returns the KV-pair and corresponding index if found
// Otherwise returns nil and the expected index of its location
func (n *Node) findLeaf(key string) (*KeyValuePair, int) {
	i := sort.Search(len(n.tuples), func(i int) bool {
		return strings.Compare(n.tuples[i].key, key) != -1
	})
	potentialTuple := n.tuples[i]

	if potentialTuple.key == key {
		return potentialTuple, i
	} else {
		return nil, i
	}
}

// insert a key-value pair into a leaf node.
func (n *Node) insert(pair *KeyValuePair) {
	if !n.isLeaf {
		panic("Tried to insert KV-Pair to non-leaf node")
	}

	// Find insertion index
	existing, index := n.findLeaf(pair.key)
	if existing != nil {
		// If key is already present, overwrite existing value
		existing.value = pair.value
	} else {
		// Otherwise, allocate space for a new KV pair
		n.tuples = append(n.tuples, &KeyValuePair{})
		copy(n.tuples[index+1:], n.tuples[index:])
		n.tuples[index] = pair
	}

	// TODO we may need to perform splitting, either here, or async on flush
}

type nodes []*Node
type tuples []*KeyValuePair
