package storage

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

type EncodablePage struct {
	ID   PageID
	Rows map[string]Row
}

type DiskReq struct {
	Page      Page
	Operation string
}

type DiskResult struct {
	Page     Page
	Response error
}

type DiskScheduler struct {
	RequestChan chan DiskReq
	ResultChan  chan DiskResult
	DiskManager *DiskManager
}

func (ds *DiskScheduler) ProccessReq() {
	for req := range ds.RequestChan {
		var result DiskResult
		fmt.Println(req)

		if req.Operation == "WRITE" {
			fmt.Println(req, "here")
			err := ds.WriteToDisk(req)
			result.Page.ID = req.Page.ID
			if err != nil {
				result.Response = errors.New("unable to write to disk: " + err.Error())
			}
		} else {
			page, err := ds.ReadFromDisk(req.Page.ID)
			result.Page = page
			if err != nil {
				result.Response = errors.New("unable to read from disk: " + err.Error())
			}
		}

		select {
		case ds.ResultChan <- result:
		default:
			fmt.Println("No listener for result")
		}
	}
}

func (ds *DiskScheduler) ReadFromDisk(ID PageID) (Page, error) {
	offset := ds.DiskManager.DirectoryPage.Mapping[ID]

	if offset == 0 {
		return Page{}, errors.New("TABLE NOT FOUND")
	}

	pageBytes := make([]byte, PageSize)
	var page Page

	_, err := ds.DiskManager.File.ReadAt(pageBytes, int64(offset))
	if err != nil {
		return page, err
	}

	endIndex := bytes.IndexByte(pageBytes, 0)

	err = json.Unmarshal(pageBytes[:endIndex], &page)
	if err != nil {
		return page, err
	}

	return page, nil
}

func (ds *DiskScheduler) WriteToDisk(req DiskReq) error {
	startPosition, err := ds.DiskManager.File.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}

	offset := ds.DiskManager.DirectoryPage.Mapping[req.Page.ID]

	pageNotFound := offset == 0
	if pageNotFound {
		firstByte, lastByte, err := ds.getPageBoundaryBytes(Offset(startPosition))
		if err != nil {
			return err
		}

		isSlotAvailable := firstByte == 0 && lastByte == 0
		if isSlotAvailable {
			pageOffset, err := ds.CreatePage(req, Offset(startPosition))
			if err != nil {
				return err
			}
			ds.DiskManager.DirectoryPage.Mapping[req.Page.ID] = pageOffset
			ds.UpdateDirectoryPage(ds.DiskManager.DirectoryPage)
		}
	} else {
		ds.writePage(req, offset)
	}

	return nil
}

func (ds *DiskScheduler) writePage(req DiskReq, offset Offset) error {
	encodablePage := EncodablePage{
		ID:   req.Page.ID,
		Rows: req.Page.Rows,
	}

	pageBytes, _ := Encode(encodablePage)

	if len(pageBytes) > PageSize {
		pageBytes = pageBytes[:PageSize]
	}

	_, err := ds.DiskManager.File.WriteAt(pageBytes, int64(offset))
	if err != nil {
		return err
	}

	return nil
}

func (ds *DiskScheduler) getPageBoundaryBytes(offset Offset) (byte, byte, error) {
	firstByte := make([]byte, 1)
	_, err := ds.DiskManager.File.ReadAt(firstByte, int64(offset))
	if err != nil && err != io.EOF {
		return 0, 0, err
	}

	pageEnd := int64(offset) + PageSize - 1
	lastByte := make([]byte, 1)
	_, err = ds.DiskManager.File.ReadAt(lastByte, pageEnd)
	if err != nil && err != io.EOF {
		return 0, 0, err
	}

	return firstByte[0], lastByte[0], nil
}

func (ds *DiskScheduler) UpdateDirectoryPage(page DirectoryPage) error {
	encodedPage, err := Encode(page)
	if err != nil {
		return err
	}

	if len(encodedPage) > DirectoryPageSize {
		return fmt.Errorf("encoded data exceeds Directory Page Size, Unable to UPDATE")
	}

	position := ds.DiskManager.HeaderSize

	_, err = ds.DiskManager.File.WriteAt(encodedPage, position)
	if err != nil {
		return err
	}

	return nil
}

func (ds *DiskScheduler) CreatePage(req DiskReq, offset Offset) (Offset, error) {
	encodablePage := EncodablePage{
		ID:   req.Page.ID,
		Rows: req.Page.Rows,
	}

	encodedPage, err := Encode(encodablePage)
	if err != nil {
		return 0, err
	}

	paddingSize := PageSize - len(encodedPage)

	buffer := append(encodedPage, make([]byte, paddingSize)...)

	n, err := ds.DiskManager.File.WriteAt(buffer, int64(offset))
	if err != nil {
		return 0, err
	}

	if n != PageSize {
		return 0, fmt.Errorf("failed to write entire page to disk")
	}

	return Offset(offset), nil
}

func (ds *DiskScheduler) AddReq(request DiskReq) {
	ds.RequestChan <- request
}

func NewDiskScheduler(dm *DiskManager) *DiskScheduler {
	diskScheduler := DiskScheduler{
		RequestChan: make(chan DiskReq),
		ResultChan:  make(chan DiskResult),
		DiskManager: dm,
	}

	return &diskScheduler
}
