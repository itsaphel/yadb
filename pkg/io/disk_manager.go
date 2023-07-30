package io

import (
	. "yadb-go/pkg/types"
)

type DiskManager interface {
	ReadPage(pageId PageId) ([]byte, error)
	FlushPage(pageId PageId, data []byte) error
}
