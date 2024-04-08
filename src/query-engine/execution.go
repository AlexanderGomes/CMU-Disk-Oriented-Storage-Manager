package queryengine

import (
	"disk-db/storage"
	"fmt"
	"hash/fnv"
)

type Query struct {
	Result  []storage.Row
	Message string
}

func ExecuteQueryPlan(qp ExecutionPlan, P *ParsedQuery, bpm *storage.BufferPoolManager) (Query, error) {
	query := Query{}

	for _, steps := range qp.Steps {
		switch steps.Operation {
		case "GetTable":

		case "GetAllColumns":

		case "FilterByColumns":

		case "InsertRows":
			InsertRows(P, &query, bpm)

		case "CreateTable":
			CreateTable(P, &query, bpm)
		}
	}

	return query, nil
}

func InsertRows(parsedQuery *ParsedQuery, query *Query, bpm *storage.BufferPoolManager) {
	pageID, _ := hashTableName(parsedQuery.TableReferences[0])
	page, _ := bpm.FetchPage(storage.PageID(pageID))
	fmt.Println(*page, "got the page")
}

func CreateTable(parsedQuery *ParsedQuery, query *Query, bpm *storage.BufferPoolManager) {
	table := parsedQuery.TableReferences[0]
	pageID, _ := hashTableName(table)
	offset := CheckIfPageExists(pageID, bpm)
	if offset != 0 {
		query.Message = "PAGE ALREADY EXISTS"
		return
	}

	CreatePage(pageID, query, bpm, parsedQuery)
}

func CreatePage(pageId uint64, query *Query, bpm *storage.BufferPoolManager, P *ParsedQuery) {
	row := storage.Row{Values: make(map[string]string)}
	page := storage.Page{
		ID:   storage.PageID(pageId),
		Rows: make(map[string]storage.Row),
	}

	for i := 0; i < len(P.ColumnsSelected); i++ {
		colum := P.ColumnsSelected[i]
		types := P.Predicates[i]
		row.Values[colum] = types
	}

	page.Rows[row.Values["ID"]] = row

	req := storage.DiskReq{
		Page:      page,
		Operation: "WRITE",
	}

	bpm.DiskManager.Scheduler.AddReq(req)
	query.Message = "TABLE CREATED"
}

func CheckIfPageExists(pageID uint64, bpm *storage.BufferPoolManager) int {
	offset := bpm.DiskManager.DirectoryPage.Mapping[storage.PageID(pageID)]
	return int(offset)
}

func hashTableName(tableName string) (uint64, error) {
	hasher := fnv.New64a()
	_, err := hasher.Write([]byte(tableName))
	if err != nil {
		return 0, err
	}

	hashValue := hasher.Sum64()

	return hashValue, nil
}
