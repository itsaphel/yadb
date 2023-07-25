package db

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
	value, exists := d.Get(key)
	assert.Equal(t, value, "world")
	assert.True(t, exists)

	d.Delete(key)
	value, exists = d.Get(key)
	assert.Equal(t, value, "")
	assert.False(t, exists)
}

func TestLoadDatabaseFromWal(t *testing.T) {
	d := LoadDatabaseFromWal("test_data/wal")

	value, exists := d.Get("key")
	assert.Equal(t, value, "")
	assert.False(t, exists)
	value, exists = d.Get("key2")
	assert.Equal(t, value, "test")
	assert.True(t, exists)
}
