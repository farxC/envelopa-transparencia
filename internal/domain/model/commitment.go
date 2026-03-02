package model

import "time"

type Commitment struct {
	ID                            int64            `db:"id"`
	CommitmentCode                string           `db:"commitment_code"`
	ResumedCommitmentCode         string           `db:"resumed_commitment_code"`
	EmissionDate                  time.Time        `db:"emission_date"`
	Type                          string           `db:"type"`
	Process                       string           `db:"process"`
	DocumentCodeType              string           `db:"document_code_type"`
	DocumentType                  string           `db:"document_type"`
	ManagementUnitName            string           `db:"management_unit_name"`
	ManagementUnitCode            int              `db:"management_unit_code"`
	ManagementCode                int              `db:"management_code"`
	ManagementName                string           `db:"management_name"`
	FavoredName                   string           `db:"favored_name"`
	FavoredCode                   string           `db:"favored_code"`
	ExpenseCategoryCode           int16            `db:"expense_category_code"`
	ExpenseCategory               string           `db:"expense_category"`
	ExpenseGroupCode              int16            `db:"expense_group_code"`
	ExpenseGroup                  string           `db:"expense_group"`
	ApplicationModalityCode       int16            `db:"application_modality_code"`
	ApplicationModality           string           `db:"application_modality"`
	ExpenseElementCode            int16            `db:"expense_element_code"`
	ExpenseElement                string           `db:"expense_element"`
	BudgetPlan                    string           `db:"budget_plan"`
	BudgetPlanCode                int32            `db:"budget_plan_code"`
	Observation                   string           `db:"observation"`
	CommitmentOriginalValue       float64          `db:"commitment_original_value"`
	CommitmentValueConvertedToBrl float64          `db:"commitment_value_converted_to_brl"`
	ConversionValueUsed           float64          `db:"conversion_value_used"`
	InsertedAt                    time.Time        `db:"inserted_at"`
	UpdatedAt                     time.Time        `db:"updated_at"`
	Items                         []CommitmentItem `db:"-" json:"items"`
}

type CommitmentItem struct {
	ID                      int64                    `db:"id"`
	CommitmentID            int64                    `db:"commitment_id"`
	CommitmentCode          string                   `db:"commitment_code"`
	ExpenseCategoryCode     int16                    `db:"expense_category_code"`
	ExpenseCategory         string                   `db:"expense_category"`
	ExpenseGroupCode        int16                    `db:"expense_group_code"`
	ExpenseGroup            string                   `db:"expense_group"`
	ApplicationModalityCode int16                    `db:"application_modality_code"`
	ApplicationModality     string                   `db:"application_modality"`
	ExpenseElementCode      int16                    `db:"expense_element_code"`
	ExpenseElement          string                   `db:"expense_element"`
	SubExpenseElement       string                   `db:"sub_expense_element"`
	SubExpenseElementCode   int16                    `db:"sub_expense_element_code"`
	Description             string                   `db:"description"`
	Quantity                float64                  `db:"quantity"`
	Sequential              int16                    `db:"sequential"`
	UnitPrice               float64                  `db:"unit_price"`
	CurrentValue            float64                  `db:"current_value"`
	CurrentPrice            float64                  `db:"current_price"`
	TotalPrice              float64                  `db:"total_price"`
	InsertedAt              time.Time                `db:"inserted_at"`
	UpdatedAt               time.Time                `db:"updated_at"`
	History                 []CommitmentItemsHistory `db:"-" json:"history"`
}

type CommitmentItemsHistory struct {
	ID             int64     `db:"id"`
	CommitmentID   int64     `db:"commitment_id"`
	CommitmentCode string    `db:"commitment_code"`
	OperationType  string    `db:"operation_type"`
	ItemQuantity   float64   `db:"item_quantity"`
	Sequential     int16     `db:"sequential"`
	ItemUnitPrice  float64   `db:"item_unit_price"`
	ItemTotalPrice float64   `db:"item_total_price"`
	OperationDate  time.Time `db:"operation_date"`
	InsertedAt     time.Time `db:"inserted_at"`
	UpdatedAt      time.Time `db:"updated_at"`
}
