package yadb

import "testing"

// Test that we can get, insert, and delete into/from a b-tree
func TestBranchOperations(t *testing.T) {
	tree := NewTree(2)

	// Ensure we can try to get a key in an empty tree
	ret := tree.Get("invalidKey")
	if ret != nil {
		t.Fatalf("Found a value for an invalid key")
	}

	// Test insert/get operations
	tree.Insert("key", "val")
	tree.Insert("key2", "val2")

	ret = tree.Get("key")
	ret2 := tree.Get("key2")
	if ret == nil || ret.key != "key" || ret.value != "val" {
		t.Fatalf("Could not retrieve value of key 'key' added to the tree")
	}
	if ret2 == nil || ret2.key != "key2" || ret2.value != "val2" {
		t.Fatalf("Could not retrieve value of key 'key2' added to the tree")
	}
	if !ret.node.isLeaf || !ret2.node.isLeaf {
		t.Fatalf("Expected key and key2 to be on leaf nodes")
	}

	// Test deletion operations
	tree.Delete("key")

	if tree.Get("key") != nil {
		t.Fatalf("Delete did not remove item from tree")
	}
}

// Test that we can get, insert, and delete into/from a b-tree
// In this test, the number of key-value pairs exceeds a single node's capacity
func TestBranchOperations__limitedCapacity(t *testing.T) {
	tree := NewTree(2)

	tree.Insert("key", "val")
	tree.Insert("key2", "val2")
	tree.Insert("key3", "val3")

	ret := tree.Get("key")
	ret2 := tree.Get("key2")
	ret3 := tree.Get("key3")
	if ret == nil || ret.key != "key" || ret.value != "val" {
		t.Fatalf("Could not retrieve value of key 'key' added to the tree")
	}
	if ret2 == nil || ret2.key != "key2" || ret2.value != "val2" {
		t.Fatalf("Could not retrieve value of key 'key2' added to the tree")
	}
	if ret3 == nil || ret3.key != "key3" || ret3.value != "val3" {
		t.Fatalf("Could not retrieve value of key 'key3' added to the tree")
	}
	if ret.node != ret2.node {
		t.Fatalf("Expected key and key2 to be on same leaf node, but they aren't.")
	}
	if ret.node == ret3.node {
		t.Fatalf("Expected key and key3 to be on separate leaf nodes, but they have the same parent.")
	}
	if ret.node == ret.node.tree.root {
		t.Fatalf("Did not expect key or key2 to be on a root node")
	}
	if !ret.node.isLeaf || !ret2.node.isLeaf {
		t.Fatalf("Expected key and key2 to be on leaf nodes")
	}

	tree.Delete("key")

	if tree.Get("key") != nil {
		t.Fatalf("Delete did not remove item from tree")
	}
}

// TODO implement
func TestRangeScan(t *testing.T) {

}

// TODO:
// 1. Inserting into blank trees should work well
// 2. Splitting and merging operations
