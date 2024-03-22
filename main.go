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
	go Pages(DB)
	ticker := time.Tick(4 * time.Second)
	for range ticker {
		go AccessPages(DB)
	}

	select {}
}

func AccessPages(DB *storage.BufferPoolManager) {
	for i := 0; i < len(DB.Pages); i++ {
		pagePtr := DB.Pages[i]
		if pagePtr != nil {
			page, _ := DB.FetchPage(pagePtr.ID)
			page, _ = DB.FetchPage(pagePtr.ID)
			page, _ = DB.FetchPage(pagePtr.ID)
			page, _ = DB.FetchPage(pagePtr.ID)
			page, _ = DB.FetchPage(pagePtr.ID)
			log.Printf("page fetched: %s", page)
		}
	}
}

// simulating DB receiving data and creating pages
func Pages(DB *storage.BufferPoolManager) {
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
