package buffer

import (
	"errors"
	"testing"

	. "yadb-go/pkg/types"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestFetchPage(t *testing.T) {
	// Given
	diskManager := new(MockDiskManager)
	mockPageData := []byte("some fake page data")
	diskManager.On("ReadPage", PageId(1)).Return(mockPageData, nil)

	pool := NewBufferPoolWithManager(diskManager)

	// When
	page := pool.FetchPage(1)

	// Then

	// Page contents should be as expected
	assert.Equal(t, page.pageId, PageId(1))
	assert.Equal(t, page.refCount, uint32(1))
	assert.Equal(t, page.data, "some fake page data")

	// And newly loaded page should be reflected in the buffer pool
	assert.Equal(t, pool.pages[0], page)
	assert.Equal(t, pool.pageTable[1], FrameId(0))
}

func TestFetchPage_FailsIfIOError(t *testing.T) {
	// Given
	diskManager := new(MockDiskManager)
	diskManager.On("ReadPage", PageId(1)).Return(nil, errors.New("IO Error"))

	pool := NewBufferPoolWithManager(diskManager)

	// When
	page := pool.FetchPage(1)

	// Then

	// Page should be nil
	assert.Nil(t, page)

	// And no changes should be reflected in the buffer pool
	assert.Nil(t, pool.pages[0])
	_, found := pool.pageTable[1]
	assert.False(t, found)
}

// Test helper objects

type MockDiskManager struct {
	mock.Mock
}

func (m *MockDiskManager) ReadPage(pageId PageId) ([]byte, error) {
	args := m.Called(pageId)
	firstArg := args.Get(0)

	if firstArg == nil {
		return nil, args.Error(1)
	} else {
		return args.Get(0).([]byte), args.Error(1)
	}
}
