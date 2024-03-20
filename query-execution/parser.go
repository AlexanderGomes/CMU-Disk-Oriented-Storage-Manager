package queryexecution

import "github.com/xwb1989/sqlparser"

type ParsedQuery struct {
	SQLStatementType string
	TableReferences  []string
	ColumnsSelected  []string
	Predicates       []string
	Joins            []string
	GroupBy          string
	OrderBy          string
	Limit            string
}
