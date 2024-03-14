package storage

type DiskReq struct {
	PageId    PageID
	Operation string
	Data      []byte
	DataIdx   int
	Offset    int64
	PageSize  uint16
}

type DiskScheduler struct {
	RequestQueue []DiskReq
	ResultChan   chan<- DiskReq
}

// background thread processing (one)
func (ds *DiskScheduler) ProccessReq() {

}

func (ds *DiskScheduler) AddReq(request DiskReq) {
	ds.RequestQueue = append(ds.RequestQueue, request)
}

func (ds *DiskScheduler) WriteDisk() {

}

func (ds *DiskScheduler) ReadDisk() {

}

// read => returning the page up
// write => sending to disk
