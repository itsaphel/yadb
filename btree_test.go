package yadb

import "testing"

func TestBranchOperations(t *testing.T) {
	tree := NewTree()

	tree.Insert("key", "val")

	ret := tree.Get("key")
	if ret == nil || ret.key != "key" || ret.value != "val" {
		t.Errorf("Could not retrieve value of a key added to the tree")
	}

	tree.Delete("key")

	if tree.Get("key") != nil {
		t.Errorf("Delete did not remove item from tree")
	}
}
