package main

import (
	"disk-db/storage"
	"log"
	"math/rand"
	"strconv"
	"time"
)

const (
	HeaderSize = 8
	k          = 2
	fileName   = "DB-file"
	rowsLimit  = 50
	numWorkers = 3
)

func main() {
	DB, err := storage.InitDatabase(k, fileName, HeaderSize)
	if err != nil {
		log.Print(err)
	}
	for i := 0; i < numWorkers; i++ {
		go DB.Worker()
	}
	go DB.DiskManager.Scheduler.ProccessReq()
	go CreatePages(DB)
	go func() {
		ticker := time.Tick(4 * time.Second)
		for range ticker {
			AcessPages(DB)
		}
	}()
	go func() {
		ticker := time.Tick(15 * time.Second)
		for range ticker {
			DB.Evict()
		}
	}()

	select {}
}

var i int
func AcessPages(DB *storage.BufferPoolManager) {
	pages := DB.Pages[i]
	page := *pages
	bufferReq := storage.BufferReq{
		Operation: "FETCH PAGE",
		PageID:    page.ID,
		Data:      []storage.Row{},
	}
	DB.AccessChan <- bufferReq

	pages2 := DB.Pages[i+1]
	page2 := *pages2
	bufferReq2 := storage.BufferReq{
		Operation: "FETCH PAGE",
		PageID:    page2.ID,
		Data:      []storage.Row{},
	}
	DB.AccessChan <- bufferReq2

	pages3 := DB.Pages[i+1]
	page3 := *pages3
	bufferReq3 := storage.BufferReq{
		Operation: "FETCH PAGE",
		PageID:    page3.ID,
		Data:      []storage.Row{},
	}
	DB.AccessChan <- bufferReq3

	i += 3
}

const (
	ID      = "ID"
	NAME    = "NAME"
	AGE     = "AGE"
	COMPANY = "COMPANY"
)

func CreatePages(DB *storage.BufferPoolManager) {
	for {
		var rows []storage.Row
		for i := 0; i < rowsLimit; i++ {
			idCounter := rand.Int63n(1000)
			idString := strconv.FormatInt(idCounter, 10)

			data := storage.Row{
				Values: map[string]string{
					ID:      idString,
					NAME:    "John Doe",
					AGE:     "22",
					COMPANY: "Acme Inc",
				},
			}
			rows = append(rows, data)
		}

		pageID := storage.PageID(time.Now().UnixNano())
		bufferReq := storage.BufferReq{
			Operation: "INSERT DATA",
			PageID:    pageID,
			Data:      rows,
		}

		DB.AccessChan <- bufferReq
		time.Sleep(time.Second)
	}
}
