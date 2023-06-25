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
// Each internal (non-leaf) node holds up to N keys and N+1 pointers to child nodes.
//
// The occupancy of the tree is the number of children as a fraction of the total
// capacity. We wish to keep a high occupancy. Splits and merges will be triggered
// to maintain this property.
//
// children are stored in sorted order to allow for binary searches.
type Node struct {
	tree *Tree

	key      string // TODO rethink this...
	parent   *Node  // Retain parent for rebalancing / splitting operations
	children nodes

	isLeaf bool

	// This is defined only for leaf pages. Must remain empty for internal nodes.
	// This is a different field because `tuples` is a different type (slice of KVPair's)
	tuples tuples
}

func (tree *Tree) NewEmptyNode(isLeaf bool) *Node {
	return &Node{
		tree:     tree,
		children: make(nodes, 0),
		tuples:   make(tuples, 0),
		isLeaf:   isLeaf,
		parent:   nil,
	}
}

type KeyValuePair struct {
	node  *Node
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

// findLeafNodeForKey finds the leaf node that should contain the key
func (n *Node) findLeafNodeForKey(key string) *Node {
	if n.isLeaf {
		return n
	}

	// Find the child internal Node under which the key would lie, if it were
	// in the tree rooted by the current node
	i := n.findInternal(key)
	return n.children[i].findLeafNodeForKey(key)
}

// findKeyInLeaf searches a leaf node for the presence of a key.
// If found, returns the leaf node, KV-pair and corresponding index
// Otherwise returns the leaf node, nil and the expected index of its location
func (n *Node) findKeyInLeaf(key string) (*KeyValuePair, int) {
	i := sort.Search(len(n.tuples), func(i int) bool {
		return strings.Compare(n.tuples[i].key, key) != -1
	})

	// if i is pointing to end of the node, then the key wasn't found
	if i >= len(n.tuples) {
		return nil, i
	}

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
	existing, index := n.findKeyInLeaf(pair.key)
	if existing != nil {
		// If key is already present, overwrite existing value
		existing.value = pair.value
	} else {
		// Otherwise, allocate space for a new KV pair
		n.tuples = append(n.tuples, &KeyValuePair{})
		copy(n.tuples[index+1:], n.tuples[index:])
		pair.node = n
		n.tuples[index] = pair
	}

	n.splitLeaf()
}

// delete removes a key from a leaf node
func (n *Node) delete(key string) {
	kv, i := n.findKeyInLeaf(key)
	// if kv == nil, the key could not be found in the tree
	// else
	if kv != nil {
		// remove the KV pair from the leaf's tuples
		n.tuples = append(n.tuples[:i], n.tuples[i+1:]...)
	}

	n.maybeMerge()
}

// Node maintenance operations

// truncate removes all items after index i, in a manner that prevents memleaks
// see https://utcc.utoronto.ca/~cks/space/blog/programming/GoSlicesMemoryLeak
func (n *Node) truncateTuples(index int) {
	for i := index + 1; i < len(n.tuples); i++ {
		n.tuples[i] = nil
	}
	n.tuples = n.tuples[:index+1]
}

func (n *Node) truncateChildren(index int) {
	for i := index + 1; i < len(n.children); i++ {
		n.children[i] = nil
	}
	n.children = n.children[:index+1]
}

// splitLeaf splits a leaf node into two, if needed
func (n *Node) splitLeaf() {
	if len(n.tuples) <= n.tree.degree {
		return
	}

	// If this is the root, we need to create a new root
	if n.parent == nil {
		n.parent = n.tree.NewEmptyNode(false)
		n.parent.children = append(n.parent.children, n)
		n.tree.root = n.parent
	}

	// Allocate a new node, transfer half of the tuples in this node to the
	// new node. Set the parent of this new node as the original node's parent.
	// Splitting is a recursive operation, the parents may need to be split.
	splitIndex := n.tree.degree / 2
	next := n.tree.NewEmptyNode(true)
	next.tuples = append(next.tuples, n.tuples[splitIndex+1:]...)
	n.truncateTuples(splitIndex)
	n.parent.children = append(n.parent.children, next)

	if n.parent != nil {
		n.parent.splitInternal()
	}
}

// splitInternal splits an internal node into two, if needed.
// Basically the same logic as splitLeaf
func (n *Node) splitInternal() {
	if len(n.children) <= n.tree.degree {
		return
	}

	if n.parent == nil {
		n.parent = n.tree.NewEmptyNode(false)
		n.parent.children = append(n.parent.children, n)
		n.tree.root = n.parent
	}

	splitIndex := n.tree.degree / 2
	next := n.tree.NewEmptyNode(false)
	next.children = append(next.children, n.children[splitIndex+1:]...)
	n.truncateChildren(splitIndex)
	n.parent.children = append(n.parent.children, next)

	if n.parent != nil {
		n.parent.splitInternal()
	}
}

// TODO impl
func (n *Node) maybeMerge() {

}

type nodes []*Node
type tuples []*KeyValuePair
