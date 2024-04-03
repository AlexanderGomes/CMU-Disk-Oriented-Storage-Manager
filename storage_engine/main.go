package main

import (
	"disk-db/storage"
	"fmt"
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
)

// # buffer pool receives requests from external world
const (
	FetchPage  = "FETCH PAGE"
	InsertData = "INSERT DATA"
)

type BufferReq struct {
	Operation string
	PageID    storage.PageID
	Data      []storage.Row
}

func main() {
	DB, err := storage.InitDatabase(k, fileName, HeaderSize)
	if err != nil {
		log.Print(err)
	}

	go DB.DiskManager.Scheduler.ProccessReq()
	go CreatePages(DB)
	go func() {
		ticker := time.Tick(10 * time.Second)
		for range ticker {
			AcessPages(DB)
			DB.Evict()
		}
	}()

	select {}
}

const (
	ID      = "ID"
	NAME    = "NAME"
	AGE     = "AGE"
	COMPANY = "COMPANY"
)

func CreatePages(DB *storage.BufferPoolManager) {
	var rows []storage.Row
	for i := 0; i < 50; i++ {
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

	for {
		pageID := storage.PageID(time.Now().UnixNano())
		err := DB.CreateAndInsertPage(rows, pageID)
		if err != nil {
			log.Printf("Error creating and inserting page: %v\n", err)
		}
		fmt.Println(rows)
		time.Sleep(time.Second)
	}
}

var i int

func AcessPages(DB *storage.BufferPoolManager) {
	var page storage.Page
	page = *DB.Pages[i]
	DB.FetchPage(page.ID)
	DB.FetchPage(page.ID)
	DB.FetchPage(page.ID)
	DB.FetchPage(page.ID)
	DB.FetchPage(page.ID)
	DB.Unpin(page.ID, false)
	log.Printf("pageID: %s", page.ID)
	i++
}
