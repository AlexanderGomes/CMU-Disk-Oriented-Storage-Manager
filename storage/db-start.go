package storage

import (
	"errors"
	"log"
)

func InitDatabase(k int, fileName string, headerSize int) (*BufferPoolManager, error) {
	bufferPool, err := NewBufferPoolManager(k, fileName, headerSize)
	if err != nil {
		log.Println("Error initializing database:", err)
		return nil, errors.New("couldn't start DB")
	}

	log.Println("Database initialized successfully")
	return bufferPool, nil
}
