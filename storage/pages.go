package storage

type PageID int64
type Page struct {
	ID       PageID
	Data     [][]byte
	IsDirty  bool
	IsPinned bool
}

type PageTest struct {
	ID       PageID
	Data     []Node
	IsDirty  bool
	IsPinned bool
}

type Row struct {
	Columns []Column
	Values  map[string]interface{}
}

type Column struct {
	Name string
	Type string
}

type Offset int64
type DirectoryPage struct {
	Mapping map[PageID]Offset
}
