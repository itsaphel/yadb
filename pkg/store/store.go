package store

import "fmt"

type Store interface {
	Get(key string) *KeyValuePair
	Set(key string, value string)
	Delete(key string)
}

type KeyValuePair struct {
	Key   string
	Value string
}

func (kv *KeyValuePair) String() string {
	if kv == nil {
		return "nil"
	}
	return fmt.Sprintf("KeyValuePair{key=%s, value=%s}", kv.Key, kv.Value)
}
