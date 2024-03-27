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
	go func() {
		ticker := time.Tick(10 * time.Second)
		for range ticker {
			AcessPages(DB)
			DB.Evict()
		}
	}()

	select {}
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

