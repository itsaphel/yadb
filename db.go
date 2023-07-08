package yadb

import (
	"yadb-go/pkg/buffer"
	"yadb-go/pkg/wal"
	"yadb-go/protoc"
)

type Database struct {
	store      map[string]string
	wal        *wal.LogFile
	bufferPool *buffer.BufferPool
}

// TODO will also need a data file path
func NewDatabase(walFileName string) *Database {
	wal := wal.NewWalFile(walFileName)

	d := &Database{
		store:      make(map[string]string),
		wal:        wal,
		bufferPool: new(buffer.BufferPool),
	}

	return d
}

func LoadDatabaseFromWal(walFileName string) *Database {
	d := NewDatabase(walFileName)
	d.wal.LoadIntoMap(d.store)

	return d
}

func (d *Database) Get(key string) string {
	return d.store[key]
}

func (d *Database) Set(key string, value string) {
	d.store[key] = value
	d.wal.Write(&protoc.WalEntry{
		Key:   key,
		Value: value,
	})
}

func (d *Database) Delete(key string) {
	delete(d.store, key)
	d.wal.Write(&protoc.WalEntry{
		Key:       key,
		Tombstone: true,
	})
}
