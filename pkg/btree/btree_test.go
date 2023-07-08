package btree

import "testing"

// Test that we can get, insert, and delete into/from a b-tree
func TestBranchOperations(t *testing.T) {
	tree := NewTree(2)

	assertKeyNotFound(t, tree, "someInvalidKey")

	// Test insert/get operations
	tree.Insert("key", "val")
	tree.Insert("key2", "val2")

	assertKeyFound(t, tree, "key", "val")
	assertKeyFound(t, tree, "key2", "val2")

	// Test deletion operations
	tree.Delete("key")
	assertKeyNotFound(t, tree, "key")
}

// Test that we can get, insert, and delete into/from a b-tree
// In this test, the number of key-value pairs exceeds a single node's capacity
func TestBranchOperations__limitedCapacity(t *testing.T) {
	tree := NewTree(2)

	tree.Insert("key", "val")
	tree.Insert("key2", "val2")
	tree.Insert("key3", "val3")

	assertKeyFound(t, tree, "key", "val")
	assertKeyFound(t, tree, "key2", "val2")
	assertKeyFound(t, tree, "key3", "val3")

	tree.Delete("key")
	assertKeyNotFound(t, tree, "key")
}

// In this test, the height is >= 2
func TestBranchOperations__largeHeight(t *testing.T) {
	tree := NewTree(2)

	tree.Insert("key", "val")
	tree.Insert("key2", "val2")
	tree.Insert("key3", "val3")
	tree.Insert("key4", "val4")
	tree.Insert("key5", "val5")
	tree.Insert("key6", "val6")

	assertKeyFound(t, tree, "key", "val")
	assertKeyFound(t, tree, "key2", "val2")
	assertKeyFound(t, tree, "key3", "val3")
	assertKeyFound(t, tree, "key4", "val4")
	assertKeyFound(t, tree, "key5", "val5")
	assertKeyFound(t, tree, "key6", "val6")

	tree.Delete("key")
	assertKeyNotFound(t, tree, "key")
}

// TODO test odd degree too

// TODO implement
func TestRangeScan(t *testing.T) {

}

func assertKeyFound(t *testing.T, tree *Tree, key string, expectedValue string) {
	res := tree.Get(key)
	if res == nil || res.key != key || res.value != expectedValue {
		t.Fatalf("Key '%s' did not have expected value '%s'. Found: %s", key, expectedValue, res.String())
	}
}

func assertKeyNotFound(t *testing.T, tree *Tree, key string) {
	res := tree.Get(key)
	if res != nil {
		t.Fatalf("Found a value for an key '%s', but expected not to.", key)
	}
}
