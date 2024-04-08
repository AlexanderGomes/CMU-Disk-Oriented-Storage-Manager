package main

import (
	"disk-db/query-engine"
	"disk-db/storage"
	//"disk-db/storage"
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

	sql := `
	INSERT INTO user (ID, Name, Age) VALUES (1, 'John Doe', 30);
	`
	parsedSQL, err := queryengine.Parser(sql)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println("PARSED QUERY:", parsedSQL)

	queryPlan, _ := queryengine.GenerateQueryPlan(parsedSQL)
	fmt.Println("QUERY PLAN:", queryPlan)

	queryResult, _ := queryengine.ExecuteQueryPlan(queryPlan, parsedSQL, DB)
	fmt.Print("QUERY RESULT:", queryResult.Message)

	select {}
}
