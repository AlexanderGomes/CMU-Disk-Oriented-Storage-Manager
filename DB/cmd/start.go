package cmd

import (
	queryengine "disk-db/DB/query-engine"
	"disk-db/DB/storage"
	"log"
)

func InitDatabase(k int, fileName string, headerSize int) (*queryengine.QueryEngine, error) {
	bufferPool, err := storage.NewBufferPoolManager(k, fileName, headerSize)
	if err != nil {
		log.Println("Error initializing database:")
		return nil, err
	}

	queryPtr := &queryengine.QueryEngine{
		DB: bufferPool,
	}

	go bufferPool.DiskManager.Scheduler.ProccessReq()
	log.Println("Database initialized successfully")
	return queryPtr, nil
}
