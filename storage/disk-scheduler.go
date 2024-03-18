package storage

import "io"

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
	_, err := ds.DiskManager.File.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}
	offset := ds.DiskManager.DirectoryPage.Mapping[req.Page.ID]

	if offset == 0 {
		ds.DiskManager.DirectoryPage.Mapping[req.Page.ID] = offset

	} else {
		//else => get page offset
	}

	return nil
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
