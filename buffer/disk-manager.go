package buffer

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"os"
)

type Offset int64
type DirectoryPage struct {
	mapping map[PageID]Offset
}

type DiskManager struct {
	file          *os.File
	directoryPage DirectoryPage
	headerSize    int64 // Size of the header in bytes
}

func NewDiskManager(filename string, headerSize int64) (*DiskManager, error) {
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}

	dm := &DiskManager{
		file:       file,
		headerSize: headerSize,
	}

	// Load directory page from file
	if err := dm.loadOrCreateDirectoryPage(); err != nil {
		return nil, err
	}

	return dm, nil
}

func (dm *DiskManager) loadOrCreateDirectoryPage() error {
	// // Check if the directory page exists in the header
	// directoryPageOffset, err := dm.readDirectoryPageOffset()
	// if err != nil {
	// 	return err
	// }

	return nil
}

func (dm *DiskManager) readDirectoryPageOffset() (Offset, error) {
	headerBytes := make([]byte, dm.headerSize)
	if _, err := dm.file.ReadAt(headerBytes, 0); err != nil {
		return 0, err
	}

	// Check if headerBytes is empty
	isEmpty := true
	for _, b := range headerBytes {
		if b != 0 {
			isEmpty = false
			break
		}
	}

	if isEmpty {
		dm.createDirectoryPage()
	} else {
		// Directory page exists, load it from file
		// if err := dm.loadDirectoryPage(directoryPageOffset); err != nil {
		// 	return err
		// }
	}

	// offsetValue := binary.BigEndian.Uint64(headerBytes[0:8])
	// return Offset(offsetValue), nil
}

func (dm *DiskManager) createDirectoryPage() {
	dm.directoryPage = DirectoryPage{
		mapping: make(map[PageID]Offset),
	}

	// Encode the directory page into a byte slice
	dirPageBytes, err := encodeDirectoryPage(dm.directoryPage)
	if err != nil {
		return err
	}

	// Write the directory page to the file after the header
	pageLocation, err := dm.writePageToFile(dirPageBytes)
	if err != nil {
		return err
	}

	// Update the header with the page location
	if err := dm.updateHeader(pageLocation); err != nil {
		return err
	}

}

func encodeDirectoryPage(page DirectoryPage) ([]byte, error) {
	// Serialize the DirectoryPage struct into a byte slice using JSON encoding
	encoded, err := json.Marshal(page)
	if err != nil {
		return nil, err // Return serialization error
	}
	return encoded, nil
}
