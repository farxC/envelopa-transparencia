package service

import "github.com/farxc/envelopa-transparencia/internal/domain/model"

const (
	OrcamentoDespesa DataType = iota + 200
)

const OrcamentoDespesaCSVSuffix = "_OrcamentoDespesa.csv"

type BudgetExtractionConfig struct {
	Codes []string
	File  string
	Year  string
}

type BudgetPayload struct {
	Year string
	// AgencyCodes are the requested subordinate agency codes. The load replaces
	// every row of Year for these agencies, including ones that left the file.
	AgencyCodes []int64
	Rows        []model.ExpenseBudget
}

type BudgetFilter struct {
	SubordinateAgencyCodes []int // one or more subordinate agency codes
	Exercise               int   // 0 = no year filter
}

type BudgetGlobalSummaryRow struct {
	ActionCode            string  `db:"action_code"             json:"action_code"`
	ActionName            string  `db:"action_name"             json:"action_name"`
	InitialBudget         float64 `db:"initial_budget"          json:"initial_budget"`
	UpdatedBudget         float64 `db:"updated_budget"          json:"updated_budget"`
	CommittedBudget       float64 `db:"committed_budget"        json:"committed_budget"`
	ExecutedBudget        float64 `db:"executed_budget"         json:"executed_budget"`
	PercentExecutedBudget float64 `db:"percent_executed_budget" json:"percent_executed_budget"`
}

type BudgetSummaryRow struct {
	ActionCode            string  `db:"action_code"             json:"action_code"`
	ActionName            string  `db:"action_name"             json:"action_name"`
	ExpenseNature         string  `db:"expense_nature"          json:"expense_nature"`
	EconomicCategory      string  `db:"economic_category"       json:"economic_category"`
	ExpenseGroupName      string  `db:"expense_group_name"      json:"expense_group_name"`
	ExpenseElementName    string  `db:"expense_element_name"    json:"expense_element_name"`
	InitialBudget         float64 `db:"initial_budget"          json:"initial_budget"`
	UpdatedBudget         float64 `db:"updated_budget"          json:"updated_budget"`
	CommittedBudget       float64 `db:"committed_budget"        json:"committed_budget"`
	ExecutedBudget        float64 `db:"executed_budget"         json:"executed_budget"`
	PercentExecutedBudget float64 `db:"percent_executed_budget" json:"percent_executed_budget"`
}

type BudgetRow struct {
	Exercise              int     `db:"exercise"                json:"exercise"`
	SuperiorOrganCode     int64   `db:"superior_organ_code"     json:"superior_organ_code"`
	SuperiorOrganName     string  `db:"superior_organ_name"     json:"superior_organ_name"`
	SubordinateAgencyCode int64   `db:"subordinate_agency_code" json:"subordinate_agency_code"`
	SubordinateAgencyName string  `db:"subordinate_agency_name" json:"subordinate_agency_name"`
	BudgetaryUnitCode     int64   `db:"budgetary_unit_code"     json:"budgetary_unit_code"`
	BudgetaryUnitName     string  `db:"budgetary_unit_name"     json:"budgetary_unit_name"`
	FunctionCode          int64   `db:"function_code"           json:"function_code"`
	FunctionName          string  `db:"function_name"           json:"function_name"`
	SubfunctionCode       int64   `db:"subfunction_code"        json:"subfunction_code"`
	SubfunctionName       string  `db:"subfunction_name"        json:"subfunction_name"`
	BudgetProgramCode     string  `db:"budget_program_code"     json:"budget_program_code"`
	BudgetProgramName     string  `db:"budget_program_name"     json:"budget_program_name"`
	ActionCode            string  `db:"action_code"             json:"action_code"`
	ActionName            string  `db:"action_name"             json:"action_name"`
	EconomicCategoryCode  int64   `db:"economic_category_code"  json:"economic_category_code"`
	EconomicCategory      string  `db:"economic_category"       json:"economic_category"`
	ExpenseGroupCode      int16   `db:"expense_group_code"      json:"expense_group_code"`
	ExpenseGroupName      string  `db:"expense_group_name"      json:"expense_group_name"`
	ExpenseElementCode    int64   `db:"expense_element_code"    json:"expense_element_code"`
	ExpenseElementName    string  `db:"expense_element_name"    json:"expense_element_name"`
	InitialBudget         float64 `db:"initial_budget"          json:"initial_budget"`
	UpdatedBudget         float64 `db:"updated_budget"          json:"updated_budget"`
	CommittedBudget       float64 `db:"committed_budget"        json:"committed_budget"`
	ExecutedBudget        float64 `db:"executed_budget"         json:"executed_budget"`
	PercentExecutedBudget float64 `db:"percent_executed_budget" json:"percent_executed_budget"`
}
