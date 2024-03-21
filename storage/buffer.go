package storage

import (
	"errors"
)

type PageID int64
type Page struct {
	ID       PageID
	Data     [][]byte
	IsDirty  bool
	IsPinned bool
}

const MaxPoolSize = 50
type FrameID int
type BufferPoolManager struct {
	Pages       [MaxPoolSize]*Page
	freeList    []FrameID
	pageTable   map[PageID]FrameID
	replacer    *LRUKReplacer
	diskManager *DiskManager // maybe use a pointer
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

	bpm.Pages[frameID] = page
	bpm.pageTable[page.ID] = frameID

	return true
}


func (bpm *BufferPoolManager) Evict() error {
	frameID, err := bpm.replacer.Evict()
	if err != nil {
		return err
	}
	page := bpm.Pages[frameID]

	req := DiskReq{
		Page:      *page,
		Operation: "WRITE",
	}

	bpm.diskManager.Scheduler.AddReq(req)
	bpm.DeletePage(page.ID)

	return nil
}

func (bpm *BufferPoolManager) DeletePage(pageID PageID) (FrameID, error) {
	if frameID, ok := bpm.pageTable[pageID]; ok {
		page := bpm.Pages[frameID]
		if page.IsPinned {
			return 0, errors.New("Page is pinned, cannot delete")
		}
		delete(bpm.pageTable, pageID)
		bpm.Pages[frameID] = nil
		bpm.freeList = append(bpm.freeList, frameID)
		return frameID, nil
	}
	return 0, errors.New("Page not found")
}

func (bpm *BufferPoolManager) FetchPage(pageID PageID) (*Page, error) {
	var page Page
	if frameID, ok := bpm.pageTable[pageID]; ok {
		page = *bpm.Pages[frameID]
		if page.IsPinned {
			return nil, errors.New("Page is pinned, cannot access")
		}
		bpm.Pin(page.ID)
	} else {
		req := DiskReq{
			Page:      page,
			Operation: "READ",
		}
		bpm.diskManager.Scheduler.AddReq(req)

		for result := range bpm.diskManager.Scheduler.ResultChan {
			if result.Page.ID == pageID {
				bpm.InsertPage(&result.Page)
				bpm.Pin(result.Page.ID)
				page = result.Page
				break
			}
		}
	}

	return &page, nil
}

func (bpm *BufferPoolManager) Unpin(pageID PageID, isDirty bool) error {
	if FrameID, ok := bpm.pageTable[pageID]; ok {
		page := bpm.Pages[FrameID]
		page.IsDirty = isDirty
		page.IsPinned = false
		return nil
	}

	return errors.New("Page Not Found")
}

func (bpm *BufferPoolManager) Pin(pageID PageID) error {
	if FrameID, ok := bpm.pageTable[pageID]; ok {
		page := bpm.Pages[FrameID]
		page.IsPinned = true

		bpm.replacer.RecordAccess(FrameID)

		return nil
	}

	return errors.New("Page Not Found")
}

func NewBufferPoolManager(k int, fileName string, headerSize int) (*BufferPoolManager, error) {
	freeList := make([]FrameID, 0)
	pages := [MaxPoolSize]*Page{}
	for i := 0; i < MaxPoolSize; i++ {
		freeList = append(freeList, FrameID(i))
		pages[FrameID(i)] = nil
	}
	pageTable := make(map[PageID]FrameID)

	replacer := NewLRUKReplacer(k)
	diskManager, err := NewDiskManager(fileName, int64(headerSize))
	if err != nil {
		return nil, err
	}

	return &BufferPoolManager{pages, freeList, pageTable, replacer, diskManager}, nil
}
