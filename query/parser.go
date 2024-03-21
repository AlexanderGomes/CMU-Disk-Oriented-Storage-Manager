package query

import (
	"github.com/xwb1989/sqlparser"
)

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

func ParseQuery(query string) (*ParsedQuery, error) {
	stmt, err := sqlparser.Parse(query)
	if err != nil {
		return nil, err
	}

	parsedQuery := &ParsedQuery{}

	switch stmt := stmt.(type) {
	case *sqlparser.Select:
		parsedQuery.SQLStatementType = "SELECT"

		if stmt.From != nil {
			sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
				switch n := node.(type) {
				case *sqlparser.AliasedTableExpr:
					tableName, ok := n.Expr.(sqlparser.TableName)
					if ok {
						parsedQuery.TableReferences = append(parsedQuery.TableReferences, tableName.Name.String())
					}

				case *sqlparser.JoinTableExpr:
					// on support
					join := sqlparser.String(n)
					parsedQuery.Joins = append(parsedQuery.Joins, join)
				}
				return true, nil
			}, stmt.From)
		}

		for _, expr := range stmt.SelectExprs {
			col, ok := expr.(*sqlparser.AliasedExpr)
			if ok {
				parsedQuery.ColumnsSelected = append(parsedQuery.ColumnsSelected, col.Expr.(*sqlparser.ColName).Name.String())
			}
		}

		if stmt.Where != nil {
			parsedQuery.Predicates = append(parsedQuery.Predicates, sqlparser.String(stmt.Where))
		}

		if stmt.GroupBy != nil {
			parsedQuery.GroupBy = sqlparser.String(stmt.GroupBy)
		}

		if stmt.OrderBy != nil {
			parsedQuery.OrderBy = sqlparser.String(stmt.OrderBy)
		}

	default:
		// Unsupported SQL statement type
		return nil, err
	}

	return parsedQuery, nil
}
