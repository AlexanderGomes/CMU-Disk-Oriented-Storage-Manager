package main

import (
	"disk-db/storage"
	"log"
	"time"
)

const HeaderSize = 8 // header to find directory page
const k = 2          // replacement policy
const fileName = "DB-file"

func main() {
	DB, err := storage.InitDatabase(k, fileName, HeaderSize)
	if err != nil {
		log.Print(err)
	}

	go DB.DiskManager.Scheduler.ProccessReq()
	go CreatePages(DB)
	ticker := time.Tick(60 * time.Second)
	for range ticker {
		AcessPages(DB)
		DB.Evict()
	}

	select {}
}

func AcessPages(DB *storage.BufferPoolManager) {
	var page storage.Page
	for i := 0; i < 20; i++ {
		page = *DB.Pages[i]
		DB.FetchPage(page.ID)
		DB.FetchPage(page.ID)
		DB.FetchPage(page.ID)
		DB.FetchPage(page.ID)
		DB.FetchPage(page.ID)
		log.Printf("pageID: %s", page.ID)
	}
}

func CreatePages(DB *storage.BufferPoolManager) {
	for {
		data := [][]byte{{byte(time.Now().Second())}}
		pageID := storage.PageID(time.Now().UnixNano())
		err := DB.CreateAndInsertPage(data, pageID)
		if err != nil {
			log.Printf("Error creating and inserting page: %v\n", err)
		}
		time.Sleep(time.Second)
	}
}
