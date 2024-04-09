package main

import (
	"disk-db/query-engine"
	"disk-db/storage"
	"fmt"
)

const (
	HeaderSize = 8
	k          = 2
	fileName   = "DB-file"
	numWorkers = 2
)

func main() {
	DB, _ := storage.InitDatabase(k, fileName, HeaderSize, numWorkers)

	sql := `SELECT ID FROM user`
	parsedSQL, err := queryengine.Parser(sql)
	if err != nil {
		fmt.Println(err)
	}

	queryPlan, _ := queryengine.GenerateQueryPlan(parsedSQL)

	queryResult, _ := queryengine.ExecuteQueryPlan(queryPlan, parsedSQL, DB)
	fmt.Print("QUERY RESULT:", queryResult)
	select {}
}
