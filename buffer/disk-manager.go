package buffer

import (
	"encoding/binary"
	"encoding/json"
	"io"
	"os"
)

type Offset int64
type DirectoryPage struct {
	Mapping map[PageID]Offset
}

type DiskManager struct {
	File          *os.File
	DirectoryPage DirectoryPage
	HeaderSize    int64 // Size in bytes
}

func NewDiskManager(filename string, headerSize int64) (*DiskManager, error) {
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}

	dm := &DiskManager{
		File:       file,
		HeaderSize: headerSize,
	}

	err = dm.SetDefaultHeader()
	if err != nil {
		return nil, err
	}

	if err := dm.loadOrCreateDirectoryPage(); err != nil {
		return nil, err
	}

	return dm, nil
}

func (dm *DiskManager) loadOrCreateDirectoryPage() error {
	directoryPageOffset, err := dm.readHeader()
	if err != nil {
		return err
	}

	if directoryPageOffset == 0 {
		if err := dm.createDirectoryPage(); err != nil {
			return err
		}
	} else {
		if err := dm.LoadDirectoryPage(directoryPageOffset); err != nil {
			return err
		}
	}

	return nil
}

func (dm *DiskManager) createDirectoryPage() error {
	dm.DirectoryPage = DirectoryPage{
		Mapping: make(map[PageID]Offset),
	}

	dirPageBytes, err := encodeDirectoryPage(dm.DirectoryPage)
	if err != nil {
		return err
	}

	pageLocation, err := dm.writePageToFile(dirPageBytes)
	if err != nil {
		return err
	}

	if err := dm.updateHeader(pageLocation); err != nil {
		return err
	}

	return nil
}

func (dm *DiskManager) readHeader() (Offset, error) {
	headerBytes := make([]byte, dm.HeaderSize)
	_, err := dm.File.ReadAt(headerBytes, 0)
	if err != nil {
		return 0, err
	}

	isDefaultHeader := true
	for _, b := range headerBytes {
		if b != 0 {
			isDefaultHeader = false
			break
		}
	}

	if isDefaultHeader {
		return 0, nil
	}

	offsetBytes := headerBytes[:8]
	offset := Offset(binary.BigEndian.Uint64(offsetBytes))

	return offset, nil
}

func (dm *DiskManager) SetDefaultHeader() error {
	fileInfo, err := dm.File.Stat()
	if err != nil {
		return err
	}
	fileSize := fileInfo.Size()

	if fileSize < dm.HeaderSize {
		if err := dm.initializeHeader(); err != nil {
			return err
		}
	}

	return nil
}

func (dm *DiskManager) initializeHeader() error {
	defaultHeader := make([]byte, dm.HeaderSize)

	_, err := dm.File.Write(defaultHeader)
	if err != nil {
		return err
	}

	return nil
}

func (dm *DiskManager) LoadDirectoryPage(offset Offset) error {
	_, err := dm.File.Seek(int64(offset), io.SeekStart)
	if err != nil {
		return err
	}

	dirPageBytes := make([]byte, PageSize)
	_, err = dm.File.Read(dirPageBytes)
	if err != nil {
		return err
	}

	err = json.Unmarshal(dirPageBytes, &dm.DirectoryPage)
	if err != nil {
		return err
	}

	return nil
}

// TASK : debug this
func (dm *DiskManager) updateHeader(offset Offset) error {
	offsetBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(offsetBytes, uint64(offset))

	headerBytes := make([]byte, dm.HeaderSize)
	if _, err := dm.File.ReadAt(headerBytes, 0); err != nil {
		return err
	}

	copy(headerBytes[dm.HeaderSize:], offsetBytes)

	_, err := dm.File.WriteAt(headerBytes, 0)
	if err != nil {
		return err
	}

	return nil
}

func (dm *DiskManager) writePageToFile(pageBytes []byte) (Offset, error) {
	offset := dm.HeaderSize
	_, err := dm.File.WriteAt(pageBytes, offset)
	if err != nil {
		return 0, err
	}

	return Offset(offset), nil
}

func encodeDirectoryPage(page DirectoryPage) ([]byte, error) {
	encoded, err := json.Marshal(page)
	if err != nil {
		return nil, err
	}
	return encoded, nil
}
