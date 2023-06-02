package yadb

import "testing"

func TestBasicApiCalls(t *testing.T) {
	d := newDatabase()

	key := "hello"
	value := "world"

	d.Set(key, value)

	if d.Get(key) != "world" {
		t.Errorf("Failed to get key from DB")
	}

	d.Delete(key)

	if d.Get(key) != "" {
		t.Errorf("Failed to delete key from DB")
	}
}
