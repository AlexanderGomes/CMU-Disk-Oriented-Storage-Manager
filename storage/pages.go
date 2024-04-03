package storage

type PageID int64
type Page struct {
	ID       PageID
	Rows     map[string]Row
	IsDirty  bool
	IsPinned bool
}

type Row struct {
	Values map[string]string
}

type Offset int64
type DirectoryPage struct {
	Mapping map[PageID]Offset
}
