package main

import (
	"disk-db/buffer"
	"fmt"
	"log"
)

const HeaderSize = 8
func main() {
	diskPtr, err := buffer.NewDiskManager("database1", HeaderSize)
	if err != nil {
		log.Println(err)
	}

	fmt.Println(diskPtr)
}
