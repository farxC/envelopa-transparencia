package model

import "time"

type ExpenseExecution struct {
	YearAndMonth                string    `db:"year_and_month"`
	SuperiorOrganCode           int32     `db:"superior_organ_code"`
	SuperiorOrganName           string    `db:"superior_organ_name"`
	SubordinatedOrganCode       int32     `db:"subordinated_organ_code"`
	SubordinatedOrganName       string    `db:"subordinated_organ_name"`
	ManagementUnitCode          int32     `db:"management_unit_code"`
	ManagementUnitName          string    `db:"management_unit_name"`
	ManagementCode              int32     `db:"management_code"`
	ManagementName              string    `db:"management_name"`
	ActionCode                  string    `db:"action_code"`
	ActionName                  string    `db:"action_name"`
	BudgetPlanCode              int32     `db:"budget_plan_code"`
	BudgetPlanName              string    `db:"budget_plan_name"`
	FederativeUnit              string    `db:"federative_unit"`
	Municipality                string    `db:"municipality"`
	AuthorAmendamentCode        int32     `db:"author_amendament_code"`
	AuthorAmendamentName        string    `db:"author_amendament_name"`
	EconomicCategoryCode        int32     `db:"economic_category_code"`
	EconomicCategoryName        string    `db:"economic_category_name"`
	ExpenseGroupCode            int32     `db:"expense_group_code"`
	ExpenseGroupName            string    `db:"expense_group_name"`
	ExpenseCategoryCode         int32     `db:"expense_category_code"`
	ExpenseCategoryName         string    `db:"expense_category_name"`
	ExpenseModalityCode         int32     `db:"expense_modality_code"`
	ExpenseModalityName         string    `db:"expense_modality_name"`
	CommittedValueBRL           float64   `db:"committed_value_brl"`
	LiquidatedValueBRL          float64   `db:"liquidated_value_brl"`
	PaidValueBRL                float64   `db:"paid_value_brl"`
	RegisteredPayablesAmountBRL float64   `db:"registered_payables_amount_brl"`
	CancelledPayablesAmountBRL  float64   `db:"canceled_payables_amount_brl"`
	PaidPayablesAmountBRL       float64   `db:"paid_payables_amount_brl"`
	InsertedAt                  time.Time `db:"inserted_at"`
	UpdatedAt                   time.Time `db:"updated_at"`
}
