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

	fmt.Println(diskPtr.DirectoryPage)
}
