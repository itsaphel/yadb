package io

import (
	"yadb-go/pkg/buffer"
)

type DiskManager interface {
	ReadPage(pageId buffer.PageId) (*buffer.Page, error)
}
