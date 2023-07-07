package yadb

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBasicApiCalls(t *testing.T) {
	file, _ := os.CreateTemp("", "yadb_wal")
	d := NewDatabase(file.Name())

	key := "hello"
	value := "world"

	d.Set(key, value)
	assert.Equal(t, d.Get(key), "world")

	d.Delete(key)
	assert.Equal(t, d.Get(key), "")
}

func TestLoadDatabaseFromWal(t *testing.T) {
	d := LoadDatabaseFromWal("test_data/wal")

	assert.Equal(t, d.Get("key"), "")
	assert.Equal(t, d.Get("key2"), "test")
}
