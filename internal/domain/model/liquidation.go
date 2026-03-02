package model

import "time"

type Liquidation struct {
	ID                      int64                           `db:"id"`
	LiquidationCode         string                          `db:"liquidation_code"`
	LiquidationCodeResumed  string                          `db:"liquidation_code_resumed"`
	LiquidationEmissionDate time.Time                       `db:"liquidation_emission_date"`
	DocumentCodeType        string                          `db:"document_code_type"`
	DocumentType            string                          `db:"document_type"`
	ManagementUnitName      string                          `db:"management_unit_name"`
	ManagementUnitCode      int                             `db:"management_unit_code"`
	ManagementCode          int                             `db:"management_code"`
	ManagementName          string                          `db:"management_name"`
	FavoredCode             string                          `db:"favored_code"`
	FavoredName             string                          `db:"favored_name"`
	ExpenseCategoryCode     int16                           `db:"expense_category_code"`
	ExpenseCategory         string                          `db:"expense_category"`
	ExpenseGroupCode        int16                           `db:"expense_group_code"`
	ExpenseGroup            string                          `db:"expense_group"`
	ApplicationModalityCode int16                           `db:"application_modality_code"`
	ApplicationModality     string                          `db:"application_modality"`
	ExpenseElementCode      int16                           `db:"expense_element_code"`
	ExpenseElement          string                          `db:"expense_element"`
	BudgetPlan              string                          `db:"budget_plan"`
	BudgetPlanCode          int32                           `db:"budget_plan_code"`
	Observation             string                          `db:"observation"`
	InsertedAt              time.Time                       `db:"inserted_at"`
	UpdatedAt               time.Time                       `db:"updated_at"`
	ImpactedCommitments     []LiquidationImpactedCommitment `db:"-" json:"impacted_commitments"`
}

type LiquidationImpactedCommitment struct {
	ID                            int64     `db:"id"`
	CommitmentCode                string    `db:"commitment_code"`
	LiquidationCode               string    `db:"liquidation_code"`
	ExpenseNatureCodeComplete     int64     `db:"expense_nature_code_complete"`
	Subitem                       string    `db:"subitem"`
	LiquidatedValueBRL            float64   `db:"liquidated_value_brl"`
	RegisteredPayablesValueBRL    float64   `db:"registered_payables_value_brl"`
	CanceledPayablesValueBRL      float64   `db:"canceled_payables_value_brl"`
	OutstandingValueLiquidatedBRL float64   `db:"outstanding_value_liquidated_brl"`
	InsertedAt                    time.Time `db:"inserted_at"`
	UpdatedAt                     time.Time `db:"updated_at"`
}
