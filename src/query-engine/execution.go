package queryengine

import (
	"disk-db/storage"
	"fmt"
	"hash/fnv"
	"strings"
)

type Query struct {
	Result  []storage.Row
	Message string
}

func ExecuteQueryPlan(qp ExecutionPlan, P *ParsedQuery, bpm *storage.BufferPoolManager) (Query, error) {
	query := Query{}
	var page *storage.Page
	for _, steps := range qp.Steps {
		switch steps.Operation {
		case "GetTable":
			page = GetTable(P, &query, bpm, steps)
		case "GetAllColumns":
			GetAllColumns(page, &query)
		case "FilterByColumns":
			FilterByColumns(page, &query, P)
		case "InsertRows":
			InsertRows(P, &query, bpm)
		case "CreateTable":
			CreateTable(P, &query, bpm)
		case "JoinQueryTable":
			JoinTables(&query, page, P.Joins[0].Condition)
		}
	}

	return query, nil
}

func JoinTables(query *Query, page *storage.Page, condition string) {
	comparisonParts := strings.Split(condition, "=")
	leftColumn := strings.TrimSpace(comparisonParts[0])
	rightColumn := strings.TrimSpace(comparisonParts[1])

	var rowSlice []storage.Row
	for _, pageRow := range page.Rows {
		pageValue := pageRow.Values[rightColumn]

		for _, queryRow := range query.Result {
			queryValue := queryRow.Values[leftColumn]

			if pageValue == queryValue {
				rowSlice = append(rowSlice, pageRow)
				rowSlice = append(rowSlice, queryRow)
				break
			}
		}
	}

	query.Result = rowSlice
}

func FilterByColumns(page *storage.Page, query *Query, P *ParsedQuery) {
	for _, row := range page.Rows {
		filteredRow := storage.Row{Values: make(map[string]string)}

		for _, columnName := range P.ColumnsSelected {
			if value, ok := row.Values[columnName]; ok {
				filteredRow.Values[columnName] = value
			}
		}

		query.Result = append(query.Result, filteredRow)
	}
}

func GetAllColumns(page *storage.Page, query *Query) {
	for _, val := range page.Rows {
		query.Result = append(query.Result, val)
	}
	query.Message = "SUCCESS"
}

func GetTable(parsedQuery *ParsedQuery, query *Query, bpm *storage.BufferPoolManager, step QueryStep) *storage.Page {
	pageID, _ := hashTableName(parsedQuery.TableReferences[step.index])
	page, err := bpm.FetchPage(storage.PageID(pageID))
	if err != nil {
		fmt.Println(err)
	}

	return page
}

func InsertRows(parsedQuery *ParsedQuery, query *Query, bpm *storage.BufferPoolManager) {
	pageID, _ := hashTableName(parsedQuery.TableReferences[0])
	page, _ := bpm.FetchPage(storage.PageID(pageID))

	for _, rowInterface := range parsedQuery.Predicates {
		val := rowInterface.(storage.Row)
		for key, value := range val.Values {
			if key == "ID" {
				page.Rows[value] = val
				break
			}
		}
	}

	req := storage.DiskReq{
		Page:      *page,
		Operation: "WRITE",
	}

	bpm.DiskManager.Scheduler.AddReq(req)
	query.Message = "INSERTED"
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
		row.Values[colum] = types.(string)
	}

	page.Rows["typeID"] = row

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
