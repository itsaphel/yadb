package yadb

import "yadb-go/protoc"

type Database struct {
	store map[string]string
	wal   walFile
}

func NewDatabase() *Database {
	wal := walFile{
		filename: "wal",
	}

	d := &Database{
		store: make(map[string]string),
		wal:   wal,
	}

	return d
}

func LoadDatabaseFromWal(walFileName string) *Database {
	wal := walFile{
		filename: walFileName,
	}

	d := &Database{
		store: make(map[string]string),
		wal:   wal,
	}
	wal.loadIntoMap(d.store)

	return d
}

func (d *Database) Get(key string) string {
	return d.store[key]
}

func (d *Database) Set(key string, value string) {
	d.store[key] = value
	d.wal.write(&protoc.WalEntry{
		Key:   key,
		Value: value,
	})
}

func (d *Database) Delete(key string) {
	delete(d.store, key)
	d.wal.write(&protoc.WalEntry{
		Key:       key,
		Tombstone: true,
	})
}
