package model

import "time"

type Payment struct {
	ID                      int64                       `db:"id"`
	PaymentCode             string                      `db:"payment_code"`
	PaymentCodeResumed      string                      `db:"payment_code_resumed"`
	PaymentEmissionDate     time.Time                   `db:"payment_emission_date"`
	DocumentCodeType        string                      `db:"document_code_type"`
	DocumentType            string                      `db:"document_type"`
	FavoredCode             string                      `db:"favored_code"`
	FavoredName             string                      `db:"favored_name"`
	ManagementUnitName      string                      `db:"management_unit_name"`
	ManagementUnitCode      int                         `db:"management_unit_code"`
	ManagementCode          int                         `db:"management_code"`
	ManagementName          string                      `db:"management_name"`
	ExpenseCategoryCode     int16                       `db:"expense_category_code"`
	ExpenseCategory         string                      `db:"expense_category"`
	ExpenseGroupCode        int16                       `db:"expense_group_code"`
	ExpenseGroup            string                      `db:"expense_group"`
	ApplicationModalityCode int16                       `db:"application_modality_code"`
	ApplicationModality     string                      `db:"application_modality"`
	ExpenseElementCode      int16                       `db:"expense_element_code"`
	ExpenseElement          string                      `db:"expense_element"`
	BudgetPlan              string                      `db:"budget_plan"`
	BudgetPlanCode          int32                       `db:"budget_plan_code"`
	Observation             string                      `db:"observation"`
	ExtraBudgetary          bool                        `db:"extra_budgetary"`
	Process                 string                      `db:"process"`
	OriginalPaymentValue    float64                     `db:"original_payment_value"`
	ConvertedPaymentValue   float64                     `db:"converted_payment_value"`
	ConversionUsedValue     float64                     `db:"conversion_used_value"`
	InsertedAt              time.Time                   `db:"inserted_at"`
	UpdatedAt               time.Time                   `db:"updated_at"`
	ImpactedCommitments     []PaymentImpactedCommitment `db:"-" json:"impacted_commitments"`
}

type PaymentImpactedCommitment struct {
	ID                         int64     `db:"id"`
	CommitmentCode             string    `db:"commitment_code"`
	PaymentCode                string    `db:"payment_code"`
	ExpenseNatureCodeComplete  int64     `db:"expense_nature_code_complete"`
	Subitem                    string    `db:"subitem"`
	PaidValueBRL               float64   `db:"paid_value_brl"`
	RegisteredPayablesValueBRL float64   `db:"registered_payables_value_brl"`
	CanceledPayablesValueBRL   float64   `db:"canceled_payables_value_brl"`
	OutstandingValuePaidBRL    float64   `db:"outstanding_value_paid_brl"`
	InsertedAt                 time.Time `db:"inserted_at"`
	UpdatedAt                  time.Time `db:"updated_at"`
}
