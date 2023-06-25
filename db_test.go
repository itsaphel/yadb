package yadb

import (
	"os"
	"testing"
)

func TestBasicApiCalls(t *testing.T) {
	file, _ := os.CreateTemp("", "yadb_wal")
	d := NewDatabase(file.Name())

	key := "hello"
	value := "world"

	d.Set(key, value)

	if d.Get(key) != "world" {
		t.Fatalf("Failed to get key from DB")
	}

	d.Delete(key)

	if d.Get(key) != "" {
		t.Fatalf("Failed to delete key from DB")
	}
}

func TestLoadDatabaseFromWal(t *testing.T) {
	d := LoadDatabaseFromWal("test_data/wal")

	if d.Get("key") != "" {
		t.Fatalf("Loaded Database is not correct (key)")
	}

	if d.Get("key2") != "test" {
		t.Fatalf("Loaded Database is not correct (key2)")
	}
}
