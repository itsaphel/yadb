package yadb

import "errors"

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
// It also pins the page by incrementing the page's refCount
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

// ReleasePage should be called after you're finished with a page.
// It will decrement the refCount, making the frame available for replacement
// Returns an error if the operation was unsuccessful
func (pool *BufferPool) ReleasePage(pageId PageId) error {
	frameId, found := pool.pageTable[pageId]
	if !found {
		return errors.New("could not find a frame containing the page")
	}

	page := pool.pages[frameId]
	page.decrementRefCount()

	return nil
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
	p.refCount++
}

func (p *Page) decrementRefCount() {
	p.refCount--
}

type FrameId int
type PageId uint64
