package main

import (
	"disk-db/query"
	"fmt"
)

const HeaderSize = 8 // header to find directory page
const k = 2          // replacement policy
const fileName = "DB-file"

func main() {
	text := `
	    SELECT u.name, o.amount
        FROM users AS u
        JOIN orders AS o ON u.id = o.user_id
        WHERE o.status = 'completed'
        GROUP BY u.name
        ORDER BY o.amount DESC
`
	parsedQuery, _ := query.ParseQuery(text)

	// Use the parsedQuery object
	// For example, print some information
	println("SQL Statement Type:", parsedQuery.SQLStatementType)
	fmt.Println("Table References:", parsedQuery.TableReferences)
	fmt.Println("Columns Selected:", parsedQuery.ColumnsSelected)
	fmt.Println("Predicates:", parsedQuery.Predicates)
	fmt.Println("Joins:", parsedQuery.Joins)
	println("Group By:", parsedQuery.GroupBy)
	println("Order By:", parsedQuery.OrderBy)
}
