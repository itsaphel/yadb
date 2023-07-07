package yadb

import "sync/atomic"

const MaxPoolSize = 10

const FrameNotFound = -1

type BufferPool struct {
	pageTable map[PageId]FrameId
	pages     [MaxPoolSize]*Page
	freeList  []FrameId // frames that are not currently in use
	// replacer  *Replacer // some kind of replacement policy
	diskManager DiskManager
}

func NewBufferPool() *BufferPool {
	diskManager := new(IODiskManager)
	return NewBufferPoolWithManager(diskManager)
}

func NewBufferPoolWithManager(diskManager DiskManager) *BufferPool {
	freeList := make([]FrameId, 0)
	pages := [MaxPoolSize]*Page{}
	for i := 0; i < MaxPoolSize; i++ {
		pages[i] = nil
		freeList = append(freeList, FrameId(i))
	}

	return &BufferPool{
		pageTable:   make(map[PageId]FrameId),
		pages:       [MaxPoolSize]*Page{},
		freeList:    freeList,
		diskManager: diskManager,
	}
}

// FetchPage returns a pointer to a Page containing the page ID.
// If the page isn't currently loaded into a frame, the buffer pool will load it in.
// It also pins the page by incrementing the frame's refCount
func (pool *BufferPool) FetchPage(pageId PageId) *Page {
	// If page is already in buffer pool, return it
	frameId, found := pool.pageTable[pageId]
	if found {
		page := pool.pages[frameId]
		page.incrementRefCount()
		return page
	}

	// Otherwise, try to load it from disk into an empty frame
	frameId = pool.getEmptyFrame()
	if frameId == FrameNotFound {
		return nil
	}

	page, err := pool.diskManager.ReadPage(pageId)
	if err != nil {
		return nil
	}
	page.incrementRefCount()
	pool.pages[frameId] = page
	pool.pageTable[pageId] = frameId

	return page
}

// FlushPage flushes a page to disk
func (pool *BufferPool) FlushPage(pageId PageId) {

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

// A Page stores some disk page in memory.
type Page struct {
	pageId   PageId
	refCount uint32 // to determine if the page should be pinned
	dirty    bool   // whether the page needs flushing to disk
	data     string
	// node     *Node  // null if the page is not yet loaded into memory
}

func (p *Page) incrementRefCount() {
	atomic.AddUint32(&p.refCount, 1)
}

func (p *Page) decrementRefCount() {
	atomic.AddUint32(&p.refCount, ^uint32(0))
}

type FrameId int
type PageId uint64
