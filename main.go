package main

import (
	"disk-db/storage"
	"fmt"
)

const HeaderSize = 8 // header to find directory page
const k = 2          // replacement policy
const fileName = "DB-file"

func main() {
	bufferPool, err := storage.InitDatabase(k, fileName, HeaderSize)
	if err != nil {
		fmt.Println(err)
	}
	data1 := [][]byte{[]byte("asasa"), []byte("sasasas")}
	pageID1 := 129102801221212
	data2 := [][]byte{[]byte("asasa"), []byte("sasasas")}
	pageID2 := 129102801221212
	data3 := [][]byte{[]byte("asasa"), []byte("sasasas")}
	pageID3 := 129102801221212

	bufferPool.CreateAndInsertPage(data1, storage.PageID(pageID1))
	bufferPool.CreateAndInsertPage(data2, storage.PageID(pageID2))
	bufferPool.CreateAndInsertPage(data3, storage.PageID(pageID3))
}
