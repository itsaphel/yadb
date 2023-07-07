package yadb

import "sync/atomic"
import "unsafe"

const MaxPoolSize = 10
const PageSizeInBytes = 8192
const FrameNotFound = -1

type FrameId int
type PageId uint64

// A Page stores some disk page in memory.
type Page struct {
	pageId   PageId
	// refCount uint32 // to determine if the page should be pinned
	dirty    bool   // whether the page needs flushing to disk
	data     *uint8 // a pointer dereferenced to a byte
	// data     string
	// node     *Node  // null if the page is not yet loaded into memory
}

type BufferPool struct {
	pool [MaxPoolSize * PageSizeInBytes]uint8
	frames [MaxPoolSize]*Page 
	pageTable map[PageId]FrameId
	// pages     [MaxPoolSize]*Page
	freeList  [MaxPoolSize]FrameId // frames that are not currently in use
	// // replacer  *Replacer // some kind of replacement policy
	// diskManager *DiskManager
}

// ConstructBufferPool constructs a buffer pool instance and return 
// the pointer to it. 
func ConstructBufferPool() *BufferPool{
	bpPtr := new(BufferPool)

	for i:= 0; i< MaxPoolSize; i++ {
		bpPtr.frames[i].data = Add(bpPtr.pool, i * PageSizeInBytes) // store main memory (start) address of each frame/page   
		bpPtr.frames[i].dirty = false 
		bpPtr.freeList[i] = i // all frames are free at the start  
	}

	return bpPtr
}


// read data field part of Page 
func (pool *BufferPool) ReadPage() {

}

// FetchPage returns a pointer to a Page containing the page ID.
// If the page isn't currently loaded into a frame, the buffer pool will load it in.
// It also pins the page by incrementing the frame's refCount
func (pool *BufferPool) FetchPage(pageId PageId) *Page {
	// If page is already in buffer pool, return it
	frameId, found := pool.pageTable[pageId]
	if found {
		pagePtr := pool.frames[frameId]
		// page.incrementRefCount()
		return pagePtr
	}

	// Otherwise, try to load it from disk into an empty frame
	frameId = pool.getEmptyFrame()											
	if frameId == FrameNotFound {
		return nil
	}

	// TODO: without calling pool.diskManager.ReadPage ? 
	// get the start address of a frames (which dereference to a page)
	 pagePtr := pool.frames[frameId] 
	 pagePtr.data = nil // placeholder, this will read from ReadPage() 
	 pagePtr.dirty = false
	 pagePtr.pageId = PageId

	// page, err := pool.diskManager.ReadPage(pageId)
	// if err != nil {
	// 	return nil
	// }
	// page.incrementRefCount()
	pool.frames[frameId] = page
	pool.pageTable[pageId] = frameId

	return page
}

// // FlushPage flushes a page to disk
// func (pool *BufferPool) FlushPage(pageId int) {

// }

func (pool *BufferPool) getEmptyFrame() FrameId {
	if len(pool.freeList) > 0 {
		frameId, newFreeList := pool.freeList[0], pool.freeList[1:]
		pool.freeList = newFreeList 

		return frameId
	} else {
		return FrameNotFound
	}
}

// func (p Page) incrementRefCount() {
// 	atomic.AddUint32(&p.refCount, 1)
// }

// func (p Page) decrementRefCount() {
// 	atomic.AddUint32(&p.refCount, ^uint32(0))
// }
