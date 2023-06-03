package yadb

import "yadb-go/protoc"

type database struct {
	store map[string]string
	wal   walFile
}

func newDatabase() *database {
	wal := walFile{
		filename: "wal",
	}

	d := &database{
		store: make(map[string]string),
		wal:   wal,
	}

	return d
}

func loadDatabaseFromWal(walFileName string) *database {
	wal := walFile{
		filename: walFileName,
	}

	d := &database{
		store: make(map[string]string),
		wal:   wal,
	}
	wal.LoadIntoMap(d.store)

	return d
}

func (d *database) Get(key string) string {
	return d.store[key]
}

func (d *database) Set(key string, value string) {
	d.store[key] = value
	d.wal.Write(&protoc.WalEntry{
		Key:   key,
		Value: value,
	})
}

func (d *database) Delete(key string) {
	delete(d.store, key)
	d.wal.Write(&protoc.WalEntry{
		Key:       key,
		Tombstone: true,
	})
}
