package yadb

type database struct {
	store map[string]string
}

func newDatabase() *database {
	d := &database{
		store: make(map[string]string),
	}

	return d
}

func (d *database) Get(key string) string {
	return d.store[key]

}

func (d *database) Set(key string, value string) {
	d.store[key] = value
}

func (d *database) Delete(key string) {
	delete(d.store, key)
}
