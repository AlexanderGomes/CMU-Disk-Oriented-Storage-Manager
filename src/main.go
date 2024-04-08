package main

import (
	"disk-db/query-engine"
	"fmt"
)

const (
	HeaderSize = 8
	k          = 2
	fileName   = "DB-file"
	rowsLimit  = 50
	numWorkers = 3
)

func main() {
	sql := `
	INSERT INTO employees (employee_id, name, department_id, city)
      VALUES (1, 'John Doe', 101, 'New York'),
       (2, 'Jane Smith', 102, 'Los Angeles'),
       (3, 'Michael Johnson', 101, 'Chicago');
	`
	tree, err := queryengine.Parser(sql)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Print(tree)
}
