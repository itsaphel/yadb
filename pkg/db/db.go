package db

import (
	"yadb-go/pkg/btree"
	"yadb-go/pkg/buffer"
	"yadb-go/pkg/wal"
	"yadb-go/protoc"
)

type Database struct {
	store      *btree.Tree
	wal        *wal.LogFile
	bufferPool *buffer.BufferPool
}

// TODO will also need a data file path
func NewDatabase(walFileName string) *Database {
	wal := wal.NewWalFile(walFileName)

	d := &Database{
		store:      btree.NewTree(10),
		wal:        wal,
		bufferPool: new(buffer.BufferPool),
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
	return ret.Value(), true
}

func (d *Database) Set(key string, value string) {
	d.store.Insert(key, value)
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
