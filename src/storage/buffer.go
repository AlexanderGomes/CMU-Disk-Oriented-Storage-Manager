package storage

import (
	"errors"
	"fmt"
	"log"
	"sync"
)

const (
	FetchPage   = "FETCH PAGE"
	InsertData  = "INSERT DATA"
	MaxPoolSize = 4000
)

type BufferReq struct {
	Operation string
	PageID    PageID
	Data      []Row
}

type BufferRes struct {
	PageID  PageID
	PagePtr *Page
	Error   error
}

type FrameID int
type BufferPoolManager struct {
	Pages       [MaxPoolSize]*Page
	freeList    []FrameID
	pageTable   map[PageID]FrameID
	Replacer    *LRUKReplacer
	DiskManager *DiskManager
	AccessChan  chan BufferReq
	ResultChan  chan BufferRes
	mu          sync.Mutex
}

func (bpm *BufferPoolManager) AddReq(req BufferReq) {
	bpm.AccessChan <- req
}

func (bpm *BufferPoolManager) Worker() {
	for req := range bpm.AccessChan {
		var result BufferRes

		switch req.Operation {
		case FetchPage:
			pagePtr, err := bpm.FetchPage(req.PageID)
			if err != nil {
				result.Error = errors.New("unable to fetch page: " + err.Error())
			}

			result.PageID = req.PageID
			result.PagePtr = pagePtr

		case InsertData:
			err := bpm.CreateAndInsertPage(req.Data, req.PageID)
			if err != nil {
				result.Error = errors.New("unable to write page: " + err.Error())
			}
			result.PageID = req.PageID
		default:
			fmt.Println("invalid request")
		}

		select {
		case bpm.ResultChan <- result:
		default:
			fmt.Println("No listener for result")
		}
	}
}

func (bpm *BufferPoolManager) CreateAndInsertPage(data []Row, ID PageID) error {
	page := &Page{
		ID:   ID,
		Rows: make(map[string]Row),
	}

	InsertRows(data, page)

	err := bpm.InsertPage(page)
	if err != nil {
		log.Print(err)
	}

	return nil
}

func InsertRows(data []Row, page *Page) {
	for _, row := range data {
		for key, value := range row.Values {
			var id string
			if key == "ID" {
				id = value
				row := Row{Values: row.Values}
				page.Rows[id] = row
				break
			}
		}
	}
}

func (bpm *BufferPoolManager) InsertPage(page *Page) error {
	if len(bpm.freeList) == 0 {
		return nil
	}
	frameID := bpm.freeList[0]
	bpm.freeList = bpm.freeList[1:]

	bpm.Pages[frameID] = page
	bpm.pageTable[page.ID] = frameID

	return nil
}

func (bpm *BufferPoolManager) Evict() error {
	frameID, err := bpm.Replacer.Evict()
	if err != nil {
		return err
	}
	page := bpm.Pages[frameID]

	req := DiskReq{
		Page:      *page,
		Operation: "WRITE",
	}

	bpm.DiskManager.Scheduler.AddReq(req)
	bpm.DeletePage(page.ID)
	fmt.Println("EVICTED")
	return nil
}

func (bpm *BufferPoolManager) DeletePage(pageID PageID) (FrameID, error) {
	if frameID, ok := bpm.pageTable[pageID]; ok {
		delete(bpm.pageTable, pageID)
		bpm.Pages[frameID] = nil
		bpm.freeList = append(bpm.freeList, frameID)
		return frameID, nil
	}
	return 0, errors.New("Page not found")
}

func (bpm *BufferPoolManager) FetchPage(pageID PageID) (*Page, error) {
	var pagePtr *Page
	bpm.mu.Lock()
	defer bpm.mu.Unlock()

	if frameID, ok := bpm.pageTable[pageID]; ok {
		pagePtr = bpm.Pages[frameID]
		if pagePtr.IsPinned {
			return nil, errors.New("Page is pinned, cannot access")
		}
		bpm.Pin(pagePtr.ID)
	} else {
		page := Page{
			ID: pageID,
		}

		req := DiskReq{
			Page:      page,
			Operation: "READ",
		}

		bpm.DiskManager.Scheduler.AddReq(req)
		for result := range bpm.DiskManager.Scheduler.ResultChan {
			if result.Page.ID == pageID {
				bpm.InsertPage(&result.Page)
				bpm.Pin(result.Page.ID)
				pagePtr = &result.Page
				break
			}
		}
	}

	return pagePtr, nil
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
		bpm.Replacer.RecordAccess(FrameID)

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

	accessChan := make(chan BufferReq)
	resultChan := make(chan BufferRes)

	return &BufferPoolManager{pages, freeList, pageTable, replacer, diskManager, accessChan, resultChan, sync.Mutex{}}, nil
}
