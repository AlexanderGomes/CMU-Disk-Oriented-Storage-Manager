package storage

import (
	"log"
)

func InitDatabase(k int, fileName string, headerSize int) (*BufferPoolManager, error) {
	bufferPool, err := NewBufferPoolManager(k, fileName, headerSize)
	if err != nil {
		log.Println("Error initializing database:")
		return nil, err
	}

	go bufferPool.DiskManager.Scheduler.ProccessReq()
	log.Println("Database initialized successfully")
	return bufferPool, nil
}
