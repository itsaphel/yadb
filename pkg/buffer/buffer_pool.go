package buffer

import (
	"errors"
	"unsafe"

	"yadb-go/pkg/io"
	. "yadb-go/pkg/types"
)

const BufferPoolCapacityInBytes = 512000000
const PageSizeInBytes = int(unsafe.Sizeof(Page{}))
const MaxPoolSize = BufferPoolCapacityInBytes / PageSizeInBytes
const FrameNotFound = -1

type BufferPool struct {
	pageTable map[PageId]FrameId
	pages     [MaxPoolSize]Page
	freeList  []FrameId // frames that are not currently in use
	// replacer  *Replacer // some kind of replacement policy
	diskManager io.DiskManager
}

func NewBufferPool() *BufferPool {
	diskManager := new(io.IODiskManager)
	return NewBufferPoolWithManager(diskManager)
}

func NewBufferPoolWithManager(diskManager io.DiskManager) *BufferPool {
	freeList := make([]FrameId, 0)
	pages := [MaxPoolSize]*Page{}
	for i := 0; i < MaxPoolSize; i++ {
		pages[i] = nil
		freeList = append(freeList, FrameId(i))
	}

	return &BufferPool{
		pageTable:   make(map[PageId]FrameId),
		pages:       [MaxPoolSize]Page{},
		freeList:    freeList,
		diskManager: diskManager,
	}
}

// FetchPage returns a pointer to a Page containing the page ID.
// It also pins the page by incrementing the page's refCount
func (pool *BufferPool) FetchPage(pageId PageId) (*Page, error) {
	// If page is already in buffer pool, return it
	frameId, found := pool.pageTable[pageId]
	if found {
		page := pool.pages[frameId]
		page.incrementRefCount()
		return &page, nil
	}

	// Otherwise, try to load it from disk into an empty frame
	frameId = pool.getEmptyFrame()
	if frameId == FrameNotFound {
		return nil, errors.New("no empty frame to load page into")
	}

	data, err := pool.diskManager.ReadPage(pageId)
	if err != nil {
		return nil, err
	}
	page := NewPage(pageId, string(data))
	page.incrementRefCount()
	pool.pages[frameId] = *page
	pool.pageTable[pageId] = frameId

	return page, nil
}

// ReleasePage should be called after you're finished with a page.
// It will decrement the refCount, making the frame available for replacement
// Returns an error if the operation was unsuccessful
func (pool *BufferPool) ReleasePage(pageId PageId) error {
	frameId, err := pool.validatePageInBuffer(pageId)
	if err != nil {
		return err
	}

	page := pool.pages[frameId]
	page.decrementRefCount()

	return nil
}

// FlushPage flushes a page to disk
func (pool *BufferPool) FlushPage(pageId PageId) error {
	frameId, found := pool.pageTable[pageId]
	if !found {
		return errors.New("requested to flush page which is not in buffer pool")
	}

	page := pool.pages[frameId]
	err := pool.diskManager.FlushPage(pageId, []byte(page.data))
	if err != nil {
		return err
	}

	return nil
}

func (pool *BufferPool) getEmptyFrame() FrameId {
	if len(pool.freeList) > 0 {
		frameId, newFreeList := pool.freeList[0], pool.freeList[1:]
		pool.freeList = newFreeList

		return frameId
	} else {
		return FrameNotFound
	}
}

// check that the given pageId is currently loaded in the buffer pool, if so
// return the frame ID, otherwise return an error
func (pool *BufferPool) validatePageInBuffer(pageId PageId) (FrameId, error) {
	frameId, found := pool.pageTable[pageId]
	if !found {
		return -1, errors.New("could not find a frame containing the page")
	}

	return frameId, nil
}

// A Page stores some disk page in memory.
type Page struct {
	pageId   PageId
	refCount uint32 // to determine if the page should be pinned
	dirty    bool   // whether the page needs flushing to disk
	data     string
	// node     *Node  // null if the page is not yet loaded into memory
}

// NewPage should be used when loading a new page into the buffer.
// The newly created page has refCount 0 and is not dirty
func NewPage(pageId PageId, data string) *Page {
	return &Page{
		pageId:   pageId,
		refCount: 0,
		dirty:    false,
		data:     data,
	}
}

func (p *Page) incrementRefCount() {
	p.refCount++
}

func (p *Page) decrementRefCount() {
	p.refCount--
}

type FrameId int
