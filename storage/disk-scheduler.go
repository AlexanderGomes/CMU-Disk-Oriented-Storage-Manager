package storage

import (
	"fmt"
	"io"
)

// import "fmt"

type DiskReq struct {
	Page      Page
	Operation string
}

type DiskScheduler struct {
	RequestChan chan DiskReq
	ResultChan  chan DiskReq
	DiskManager *DiskManager
}

// switch case
func (ds *DiskScheduler) ProccessReq() {
	for req := range ds.RequestChan {
		if req.Operation == "WRITE" {
			ds.WriteToDisk(req)
		} else {
			ds.ReadFromDisk(req)
		}
	}
}

func (ds *DiskScheduler) WriteToDisk(req DiskReq) error {
	startPosition, err := ds.DiskManager.File.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}

	offset := ds.DiskManager.DirectoryPage.Mapping[req.Page.ID]

	if offset == 0 {

		firstByte, lastByte, err := ds.getFistAndLastByte(Offset(startPosition))
		if err != nil {
			return err
		}

		isSlotAvailable := firstByte == 0 && lastByte == 0

		if isSlotAvailable {
			pageOffset, err := ds.CreatePage(req.Page, Offset(startPosition))
			if err != nil {
				return err
			}

			//update in-memory and in-disk directory
			ds.DiskManager.DirectoryPage.Mapping[req.Page.ID] = pageOffset
			ds.UpdateDirectoryPage(ds.DiskManager.DirectoryPage)
		} else {
			// change start position, start everything over again
		}
	}

	return nil

}

func (ds *DiskScheduler) getFistAndLastByte(offset Offset) (byte, byte, error) {
	firstByte := make([]byte, 1)
	_, err := ds.DiskManager.File.ReadAt(firstByte, int64(offset))
	if err != nil && err != io.EOF {
		return 0, 0, err
	}

	pageEnd := int64(offset) + PageSize
	lastByte := make([]byte, 1)
	_, err = ds.DiskManager.File.ReadAt(lastByte, pageEnd-1)
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
		return fmt.Errorf("encoded data size exceeds 2KB")
	}

	position := ds.DiskManager.HeaderSize

	_, err = ds.DiskManager.File.WriteAt(encodedPage, position)
	if err != nil {
		return err
	}

	return nil
}

func (ds *DiskScheduler) CreatePage(page Page, offset Offset) (Offset, error) {
	encodedPage, err := Encode(page)
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

func (ds *DiskScheduler) ReadFromDisk(req DiskReq) {

}

func (ds *DiskScheduler) AddReq(request DiskReq) {
	ds.RequestChan <- request
}

func NewDiskScheduler(dm *DiskManager) *DiskScheduler {
	diskScheduler := DiskScheduler{
		RequestChan: make(chan DiskReq),
		ResultChan:  make(chan DiskReq),
		DiskManager: dm,
	}

	go diskScheduler.ProccessReq()

	return &diskScheduler
}
