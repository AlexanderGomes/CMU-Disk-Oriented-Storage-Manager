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
	Data [][]byte
}

type DiskReq struct {
	Page      Page
	Operation string
}

type DiskScheduler struct {
	RequestChan chan DiskReq
	ResultChan  chan DiskResult
	DiskManager *DiskManager
}

type DiskResult struct {
	Page     Page
	Response error
}

func (ds *DiskScheduler) ProccessReq() {
	for req := range ds.RequestChan {
		var result DiskResult
		if req.Operation == "WRITE" {
			err := ds.WriteToDisk(req)
			result.Page.ID = req.Page.ID
			if err != nil {
				result.Response = errors.New("unable to write to disk: " + err.Error())
			}

			result.Response = nil
		} else {
			page, err := ds.ReadFromDisk(req.Page.ID)
			result.Page = page
			if err != nil {
				result.Response = errors.New("unable to read from disk: " + err.Error())
			}

			result.Response = nil
		}
		ds.ResultChan <- result
	}
}

func (ds *DiskScheduler) ReadFromDisk(ID PageID) (Page, error) {
	offset := ds.DiskManager.DirectoryPage.Mapping[ID]
	pageBytes := make([]byte, PageSize)
	page := Page{}

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

	//#page doesn't exist
	if offset == 0 {
		firstByte, lastByte, err := ds.getPageBoundaryBytes(Offset(startPosition))
		if err != nil {
			return err
		}

		isSlotAvailable := firstByte == 0 && lastByte == 0
		if isSlotAvailable {
			pageOffset, err := ds.CreatePage(req.Page, Offset(startPosition))
			if err != nil {
				return err
			}
			//update in-memory and on-disk directory
			ds.DiskManager.DirectoryPage.Mapping[req.Page.ID] = pageOffset
			ds.UpdateDirectoryPage(ds.DiskManager.DirectoryPage)
		}
	} else {
		encodablePage := EncodablePage{
			ID:   req.Page.ID,
			Data: req.Page.Data,
		}
		pageBytes, _ := Encode(encodablePage)
		_, err := ds.DiskManager.File.WriteAt(pageBytes, int64(offset))
		if err != nil {
			return err
		}
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

	if len(encodedPage) > PageSize {
		return fmt.Errorf("encoded data exceeds 2KB")
	}

	position := ds.DiskManager.HeaderSize

	_, err = ds.DiskManager.File.WriteAt(encodedPage, position)
	if err != nil {
		return err
	}

	return nil
}

func (ds *DiskScheduler) CreatePage(page Page, offset Offset) (Offset, error) {
	encodablePage := EncodablePage{
		ID:   page.ID,
		Data: page.Data,
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
