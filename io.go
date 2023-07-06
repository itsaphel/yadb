package yadb

import (
	"log"
	"os"
)

const PageSizeInBytes = 8192 // 8kB

type DiskManager struct {
	filename string
}

func (d DiskManager) ReadPage(pageId PageId) (*Page, error) {
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

	page := &Page{
		pageId:   pageId,
		refCount: 0,
		dirty:    false,
		data:     string(data),
	}
	return page, nil
}
