package main

import (
	"disk-db/storage"
	"fmt"
	"log"
)

const HeaderSize = 8

func main() {

	diskPtr, err := storage.NewDiskManager("db-file", HeaderSize)
	if err != nil {
		log.Println(err)
	}

	data := [][]byte{[]byte("ajsabjskj skas as jka skj asj ajk sjk asj")}

	req := storage.DiskReq{
		Page: storage.Page{
			ID:   129192912,
			Data: data,
		},
	}

	page, _ := diskPtr.Scheduler.ReadFromDisk(req.Page.ID)
	fmt.Println(page)
	fmt.Println(diskPtr.DirectoryPage)
}
