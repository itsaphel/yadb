package yadb

import "testing"

func TestBranchOperations(t *testing.T) {
	tree := NewTree()

	tree.Insert("key", "val")

	if tree.root.get("key").value != "val" {
		t.Errorf("Could not retrieve value of a key added to the tree")
	}

	tree.Delete("key")

	if tree.root.get("key") != nil {
		t.Errorf("Delete did not remove item from tree")
	}
}
