package yadb

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestFetchPage(t *testing.T) {
	// Given
	diskManager := new(MockDiskManager)
	mockPage := &Page{
		pageId:   1,
		refCount: 0,
		dirty:    false,
		data:     "some fake page data",
	}
	diskManager.On("ReadPage", PageId(1)).Return(mockPage, nil)

	pool := NewBufferPoolWithManager(diskManager)

	// When
	page := pool.FetchPage(1)

	// Then

	// Page contents should be as expected
	assert.Equal(t, page.pageId, PageId(1))
	assert.Equal(t, page.refCount, uint32(1))
	assert.Equal(t, page.data, "some fake page data")

	// And newly loaded page should be reflected in the buffer pool
	assert.Equal(t, pool.pages[0], mockPage)
	assert.Equal(t, pool.pageTable[1], FrameId(0))
}

// Test helper objects

type MockDiskManager struct {
	mock.Mock
}

func (m MockDiskManager) ReadPage(pageId PageId) (*Page, error) {
	args := m.Called(pageId)
	return args.Get(0).(*Page), args.Error(1)
}
