package db

import (
	"yadb-go/pkg/buffer"
	"yadb-go/pkg/store"
	"yadb-go/pkg/store/inmemory-btree"
	"yadb-go/pkg/wal"
	"yadb-go/protoc"
)

type Database struct {
	store      store.Store
	wal        *wal.LogFile
	bufferPool *buffer.BufferPool
}

// TODO will also need a data file path
func NewDatabase(walFileName string) *Database {
	wal := wal.NewWalFile(walFileName)

	d := &Database{
		store:      inmemory_btree.NewTree(10),
		wal:        wal,
		bufferPool: buffer.NewBufferPool(),
	}

	return d
}

func LoadDatabaseFromWal(walFileName string) *Database {
	d := NewDatabase(walFileName)
	d.wal.ReplayIntoStore(d.store)

	return d
}

func (d *Database) Get(key string) (string, bool) {
	ret := d.store.Get(key)
	if ret == nil {
		return "", false
	}
	return ret.Value, true
}

func (d *Database) Set(key string, value string) {
	d.store.Set(key, value)
	d.wal.Write(&protoc.WalEntry{
		Key:   key,
		Value: value,
	})
}

func (d *Database) Delete(key string) {
	d.store.Delete(key)
	d.wal.Write(&protoc.WalEntry{
		Key:       key,
		Tombstone: true,
	})
}
