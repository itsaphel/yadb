// This file implements B+ Trees for use in a database system.
//
// All KV-pairs are stored solely on the leaf level. All operations (like inserting,
// updating, removing and retrieval) affect only leaf nodes. They propagate to
// higher levels only during splits and merges.
//
// In a B+ Tree, we access at most log_C(N) pages (where N is number of tuples
// and C is node capacity). The number of comparisons is log_2(N)

package yadb

import (
	"fmt"
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

// Node represents a node in a B+ Tree.
// Each internal node holds up to N keys and N+1 pointers to child nodes.
// Each leaf node holds N pointers to heap data
//
// The occupancy of the tree is the number of children as a fraction of the total
// capacity. We wish to keep a high occupancy. Splits and merges will be triggered
// to maintain this property.
//
// Keys are stored in sorted order to allow for binary searches.
type Node struct {
	tree   *Tree
	parent *Node // Retain parent for rebalancing / splitting operations

	keys     []string      // Only set for internal nodes.
	pointers []interface{} // For internal nodes, points to other internal nodes. For leaves, points to KV-pairs

	isLeaf bool
}

func (tree *Tree) NewEmptyNode(isLeaf bool) *Node {
	return &Node{
		tree:     tree,
		parent:   nil,
		keys:     make([]string, 0), // TODO maybe size shouldn't be 0 - resizing time
		pointers: make([]interface{}, 0),
		isLeaf:   isLeaf,
	}
}

type KeyValuePair struct {
	key   string
	value string
}

func (kv *KeyValuePair) String() string {
	if kv == nil {
		return "nil"
	}
	return fmt.Sprintf("KeyValuePair{key=%s, value=%s}", kv.key, kv.value)
}

// get returns a pointer to the KV Pair if the key exists under this node (or
// its children, recursively), otherwise returns nil.
func (n *Node) get(key string) *KeyValuePair {
	leaf := n.findLeafNodeForKey(key)

	// try to find the specific KV pair in the leaf node's tuples
	pair, _ := leaf.findKeyInLeaf(key)
	return pair
}

// findIndex returns the index of the child node which should contain the key,
// and whether there was an exact match
// TODO we probably don't need the exact match boolean anymore, as the new
// version of this method is only ran on internal nodes.
func (n *Node) findIndex(key string) (int, bool) {
	var exact bool
	i := sort.Search(len(n.keys), func(i int) bool {
		ret := strings.Compare(n.keys[i], key)
		if ret == 0 {
			exact = true
		}
		return ret == 1
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
		ret := strings.Compare(n.pointers[i].(*KeyValuePair).key, key)
		if ret == 0 {
			exact = true
		}
		return ret >= 0
	})

	if exact {
		return n.pointers[i].(*KeyValuePair), i
	} else {
		return nil, i
	}
}

// putKey promotes a key to an internal node
func (n *Node) putKey(key string, pointer *Node) {
	i, _ := n.findIndex(key)

	n.keys = append(n.keys, "")
	n.pointers = append(n.pointers, nil)

	// make space if index is not at the end
	if i < len(n.keys) {
		copy(n.keys[i+1:], n.keys[i:])
		copy(n.pointers[i+2:], n.pointers[i+1:])
	}

	n.keys[i] = key
	n.pointers[i+1] = pointer
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
		n.pointers = append(n.pointers, nil)
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
		// n.keys = append(n.keys[:i], n.keys[i+1:]...)
		n.pointers = append(n.pointers[:i], n.pointers[i+1:]...)
	}

	n.maybeMerge()
}

// Node maintenance operations

// truncate functions remove items after & including index i,
// in a manner that prevents memleaks
// see https://utcc.utoronto.ca/~cks/space/blog/programming/GoSlicesMemoryLeak

func (n *Node) truncateKeys(index int) {
	n.keys = n.keys[:index]
}

func (n *Node) truncatePointers(index int) {
	for i := index; i < len(n.pointers); i++ {
		n.pointers[i] = nil
	}
	n.pointers = n.pointers[:index]
}

// splitLeaf splits a leaf node into two, if needed
func (n *Node) split() {
	if len(n.keys) <= n.tree.degree {
		return
	}

	// Allocate a new node, transfer half of the tuples in this node to the
	// new node. Set the parent of this new node as the original node's parent.
	// Splitting is a recursive operation; the parents may also need to be split.

	// Move items from index N/2 onwards to new node
	splitIndex := n.tree.degree / 2
	var splitIndexKey string
	if n.isLeaf {
		splitIndexKey = n.pointers[splitIndex].(*KeyValuePair).key
	} else {
		splitIndexKey = n.keys[splitIndex]
	}
	next := n.tree.NewEmptyNode(n.isLeaf)
	next.pointers = append(next.pointers, n.pointers[splitIndex:]...)
	if !n.isLeaf {
		next.keys = append(next.keys, n.keys[splitIndex+1:]...) // intentionally not including key in next node, as it has been promoted
		n.truncateKeys(splitIndex)
	}
	n.truncatePointers(splitIndex)

	// If this is the root, we need to create a new root
	if n.parent == nil {
		newRoot := n.tree.NewEmptyNode(false)
		newRoot.pointers = append(newRoot.pointers, n)
		n.tree.root = newRoot
		n.parent = newRoot
	}
	next.parent = n.parent
	// Add the split index to the parent node
	n.parent.putKey(splitIndexKey, next)
	n.parent.split()
}

// TODO impl
func (n *Node) maybeMerge() {

}
