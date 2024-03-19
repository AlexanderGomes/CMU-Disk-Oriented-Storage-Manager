package storage

import (
	"errors"
)

const MaxPoolSize = 50

type PageID int64
type Page struct {
	ID       PageID
	Data     [][]byte
	IsDirty  bool
	IsPinned bool
}

type FrameID int
type BufferPoolManager struct {
	pages     [MaxPoolSize]*Page
	freeList  []FrameID
	pageTable map[PageID]FrameID
}

func (bpm *BufferPoolManager) CreateAndInsertPage(data [][]byte, ID PageID) error {
	page := &Page{
		ID:   ID,
		Data: data,
	}

	isAdded := bpm.InsertPage(page)
	if !isAdded {
		return errors.New("unable to add page to buffer pool")
	}

	return nil
}

func (bpm *BufferPoolManager) InsertPage(page *Page) bool {
	if len(bpm.freeList) == 0 {
		return false
	}

	frameID := bpm.freeList[0]
	bpm.freeList = bpm.freeList[1:]

	bpm.pages[frameID] = page
	bpm.pageTable[page.ID] = frameID

	return true
}

func (bpm *BufferPoolManager) DeletePage(pageID PageID) error {
	if frameID, ok := bpm.pageTable[pageID]; ok {
		page := bpm.pages[frameID]
		if page.IsPinned {
			return errors.New("Page is pinned, cannot delete")
		}
		delete(bpm.pageTable, pageID)
		bpm.pages[frameID] = nil
		bpm.freeList = append(bpm.freeList, frameID)
		return nil
	}
	return errors.New("Page not found")
}

func (bpm *BufferPoolManager) FetchPage(pageID PageID) (*Page, error) {
	var page Page
	if frameID, ok := bpm.pageTable[pageID]; ok {
		page = *bpm.pages[frameID]
		if page.IsPinned {
			return nil, errors.New("Page is pinned, cannot access")
		}
		bpm.Pin(pageID)
		return &page, nil
	}

	return &page, nil
}

func (bpm *BufferPoolManager) Unpin(pageID PageID, isDirty bool) error {
	if FrameID, ok := bpm.pageTable[pageID]; ok {
		page := bpm.pages[FrameID]
		page.IsDirty = isDirty
		page.IsPinned = false
		return nil
	}

	return errors.New("Page Not Found")
}

func (bpm *BufferPoolManager) Pin(pageID PageID) error {
	if FrameID, ok := bpm.pageTable[pageID]; ok {
		page := bpm.pages[FrameID]
		page.IsPinned = true
		// #replacemet policy

		return nil
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
