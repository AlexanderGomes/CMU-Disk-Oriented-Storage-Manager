package buffer

import (
	"errors"
)

const MaxPoolSize = 50

type PageID int
type Page struct {
	ID       PageID
	Data     []byte
	IsDirty  bool
	IsPinned bool
}

type FrameID int
type BufferPoolManager struct {
	pages     [MaxPoolSize]*Page
	freeList  []FrameID
	pageTable map[PageID]FrameID
}

func (bpm *BufferPoolManager) NewPage(pageID PageID, data []byte) *Page {
	return &Page{
		ID:       pageID,
		Data:     data,
		IsDirty:  false,
		IsPinned: false,
	}
}

func (bpm *BufferPoolManager) FetchPage(pageID PageID) *Page {
	var page Page
	//# buffer hit
	if frameID, ok := bpm.pageTable[pageID]; ok {
		page = *bpm.pages[frameID]
		bpm.Pin(pageID)
		return &page
	}

	//#disk read
	return &page
}

func (bpm *BufferPoolManager) Unpin(pageID PageID, isDirty bool) error {
	if FrameID, ok := bpm.pageTable[pageID]; ok {
		page := bpm.pages[FrameID]
		page.IsDirty = isDirty
		page.IsPinned = false
	}

	return errors.New("Page Not Found")
}

func (bpm *BufferPoolManager) Pin(pageID PageID) error {
	if FrameID, ok := bpm.pageTable[pageID]; ok {
		page := bpm.pages[FrameID]
		page.IsPinned = true
		// #replacemet policy
	}

	return errors.New("Page Not Found")
}

func NewBufferPoolManager() *BufferPoolManager {
	freeList := make([]FrameID, 0)
	pages := [MaxPoolSize]*Page{}
	for i := 0; i < MaxPoolSize; i++ {
		freeList = append(freeList, FrameID(i))
		pages[FrameID(i)] = nil
	}

	pageTable := make(map[PageID]FrameID)
	return &BufferPoolManager{pages, freeList, pageTable}
}

// FlushPage(page_id_t page_id) //check if it's pinned
// NewPage(page_id_t* page_id)
// DeletePage(page_id_t page_id) //check if it's pinned
// FlushAllPages() //check if it's pinned
