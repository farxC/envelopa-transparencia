package service

import "github.com/farxc/envelopa-transparencia/internal/domain/model"

// Execution aggregate data — despesas-execucao/{year}{month}
// Distinct from the granular lifecycle data (empenho/liquidação/pagamento).
// Source: different endpoint, monthly aggregated structure, suffix: _Despesas.csv
const (
	DespesasExecucao DataType = iota + 100
)

const (
	DespesasExecucaoDataType = "_Despesas.csv"
)

var ExecutionDataTypeNames = map[DataType]string{
	DespesasExecucao: "Despesas Execução",
}

type OutputExpensesExecutionExtractionFiles struct {
	Month string
	Year  string
	File  string
}

type UnitExpenseExecution struct {
	UgCode           int32                  `json:"ug_code"`
	UgName           string                 `json:"ug_name"`
	ExpenseExecution model.ExpenseExecution `json:"expense_execution"`
}

type ExpensesExecutionPayload struct {
	ExtractionDate string
	UnitsExpenses  []UnitExpenseExecution
}
