package io

import (
	"log"
	"os"
	. "yadb-go/pkg/types"
)

const PageSizeInBytes = 8192 // 8kB

type IODiskManager struct {
	filename string
}

func (d *IODiskManager) ReadPage(pageId PageId) ([]byte, error) {
	f, err := os.OpenFile(d.filename, os.O_RDONLY, 0644)
	if err != nil {
		log.Fatalln("Failed to open data file.", err)
	}
	defer f.Close()

	data := make([]byte, PageSizeInBytes)
	_, err = f.ReadAt(data, int64(pageId)*PageSizeInBytes)
	if err != nil {
		return nil, err
	}

	return data, nil
}
