package yadb

import "testing"

func TestBranchOperations(t *testing.T) {
	tree := NewTree()

	tree.root.insert("key", "val")

	if tree.root.get("key").value != "val" {
		t.Errorf("Could not retrieve value of a key added to the tree")
	}

	tree.root.delete("key")

	if tree.root.get("key") != nil {
		t.Errorf("Delete did not remove item from tree")
	}
}
