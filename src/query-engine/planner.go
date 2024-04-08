package queryengine

type ExecutionPlan struct {
	Steps []QueryStep
}

type QueryStep struct {
	Operation string
}

func GenerateQueryPlan(parsedQuery *ParsedQuery) (ExecutionPlan, error) {
	executionPlan := ExecutionPlan{}
	executionPlan.Steps = make([]QueryStep, 0)

	switch parsedQuery.SQLStatementType {
	case "CREATE TABLE":
		CreateTablePlan(&executionPlan, parsedQuery)
	case "INSERT":
		InsertTablePlan(&executionPlan, parsedQuery)
	case "SELECT":
		SelectTablePlan(&executionPlan, parsedQuery)
	}

	return executionPlan, nil
}

func SelectTablePlan(executionPlan *ExecutionPlan, P *ParsedQuery) {
	filterOperation := determineFilterOperation(P.ColumnsSelected)

	querySteps := []QueryStep{
		{Operation: "GetTable"},
		{Operation: filterOperation},
	}

	executionPlan.Steps = append(executionPlan.Steps, querySteps...)
}

func InsertTablePlan(executionPlan *ExecutionPlan, P *ParsedQuery) {
	querySteps := []QueryStep{
		{Operation: "InsertRows"},
	}

	executionPlan.Steps = append(executionPlan.Steps, querySteps...)
}

func CreateTablePlan(executionPlan *ExecutionPlan, P *ParsedQuery) {
	querySteps := []QueryStep{
		{Operation: "CreateTable"},
	}

	executionPlan.Steps = append(executionPlan.Steps, querySteps...)
}

func determineFilterOperation(columnsSelected []string) string {
	if len(columnsSelected) > 0 && columnsSelected[0] == "*" {
		return "GetAllColumns"
	}
	return "FilterByColumns"
}
