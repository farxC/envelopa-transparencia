package store

import (
	"time"
)

// Payment represents the 'payments' table.
type Payment struct {
	ID                    int64     `db:"id"`
	PaymentCode           string    `db:"payment_code"`
	PaymentCodeResumed    string    `db:"payment_code_resumed"`
	PaymentEmissionDate   time.Time `db:"payment_emission_date"`
	DocumentCodeType      string    `db:"document_code_type"`
	DocumentType          string    `db:"document_type"`
	FavoredCode           int64     `db:"favored_code"`
	FavoredName           string    `db:"favored_name"`
	ManagementUnitName    string    `db:"management_unit_name"`
	ManagementUnitCode    int       `db:"management_unit_code"`
	ManagementCode        int       `db:"management_code"`
	ManagementName        string    `db:"management_name"`
	ExtraBudgetary        bool      `db:"extra_budgetary"`
	Process               string    `db:"process"`
	OriginalPaymentValue  float64   `db:"original_payment_value"`
	ConvertedPaymentValue float64   `db:"converted_payment_value"`
	ConversionUsedValue   float64   `db:"conversion_used_value"`
	InsertedAt            time.Time `db:"inserted_at"`
	UpdatedAt             time.Time `db:"updated_at"`
}

// PaymentImpactedCommitment represents the 'payment_impacted_commitments' table.
// Inferred table name from "payment_impacted_commitm...".
type PaymentImpactedCommitment struct {
	ID                         int64     `db:"id"`
	CommitmentCode             string    `db:"commitment_code"`
	PaymentCode                string    `db:"payment_code"`
	ExpenseNatureCodeComplete  string    `db:"expense_nature_code_complete"`
	Subitem                    string    `db:"subitem"`
	PaidValueBRL               float64   `db:"paid_value_brl"`
	RegisteredPayablesValueBRL float64   `db:"registered_payables_value_brl"`
	CanceledPayablesValueBRL   float64   `db:"canceled_payables_value_brl"`
	OutstandingValuePaidBRL    float64   `db:"outstanding_value_paid_brl"`
	InsertedAt                 time.Time `db:"inserted_at"`
	UpdatedAt                  time.Time `db:"updated_at"`
}

// Liquidation represents the 'liquidations' table.
type Liquidation struct {
	ID                      int64     `db:"id"`
	LiquidationCode         string    `db:"liquidation_code"`
	LiquidationCodeResumed  string    `db:"liquidation_code_resumed"`
	LiquidationEmissionDate time.Time `db:"liquidation_emission_date"`
	DocumentCodeType        string    `db:"document_code_type"`
	DocumentType            string    `db:"document_type"`
	ManagementUnitName      string    `db:"management_unit_name"`
	ManagementUnitCode      int       `db:"management_unit_code"`
	ManagementCode          int       `db:"management_code"`
	ManagementName          string    `db:"management_name"`
	FavoredCode             int64     `db:"favored_code"`
	FavoredName             string    `db:"favored_name"`
	Observation             string    `db:"observation"`
	InsertedAt              time.Time `db:"inserted_at"`
	UpdatedAt               time.Time `db:"updated_at"`
}

// LiquidationImpactedCommitment represents the 'liquidation_impacted_commitments' table.
// The table name in the image is truncated to "liquidation_impac...", but
// matches the pattern of "payment_impacted_commitments".
type LiquidationImpactedCommitment struct {
	ID                            int64     `db:"id"`
	CommitmentCode                string    `db:"commitment_code"`
	LiquidationCode               string    `db:"liquidation_code"`
	ExpenseNatureCode             string    `db:"expense_nature_code"` // Inferred from "expense_nature_cod..."
	Subitem                       string    `db:"subitem"`
	LiquidatedValueBRL            float64   `db:"liquidated_value_brl"`
	RegisteredPayablesValueBRL    float64   `db:"registered_payables_value_brl"`    // Inferred from "registered_payables..."
	CanceledPayablesValueBRL      float64   `db:"canceled_payables_value_brl"`      // Inferred from "canceled_payables_..."
	OutstandingValueLiquidatedBRL float64   `db:"outstanding_value_liquidated_brl"` // Inferred from "outstanding_value_li..."
	InsertedAt                    time.Time `db:"inserted_at"`
	UpdatedAt                     time.Time `db:"updated_at"`
}

// Commitment represents the 'commitments' table.
type Commitment struct {
	ID                            int64     `db:"id"`
	CommitmentCode                string    `db:"commitment_code"`
	ResumedCommitmentCode         string    `db:"resumed_commitment_code"` // Inferred from "resumed_commitmen..."
	EmissionDate                  time.Time `db:"emission_date"`
	Type                          string    `db:"type"`
	Process                       string    `db:"process"`
	DocumentCodeType              string    `db:"document_code_type"`
	DocumentType                  string    `db:"document_type"`
	ManagementUnitName            string    `db:"management_unit_name"`
	ManagementUnitCode            int       `db:"management_unit_code"`
	ManagementCode                int       `db:"management_code"`
	ManagementName                string    `db:"management_name"`
	FavoredName                   string    `db:"favored_name"`
	ExpenseNature                 string    `db:"expense_nature"`
	ExpenseNatureCode             string    `db:"expense_nature_code"` // Inferred from "expense_nature_cod..."
	BudgetPlan                    string    `db:"budget_plan"`
	CommitmentOriginalValue       float64   `db:"commitment_original_value"`         // Inferred from "commitment_original..."
	CommitmentValueConvertedToBrl float64   `db:"commitment_value_converted_to_brl"` // Inferred from "commitment_value_c..."
	ConversionValueUsed           float64   `db:"conversion_value_used"`             // Inferred from "conversion_value_us..."
	InsertedAt                    time.Time `db:"inserted_at"`
	UpdatedAt                     time.Time `db:"updated_at"`
}

// CommitmentItem represents the 'commitment_items' table.
type CommitmentItem struct {
	ID                int64     `db:"id"`
	CommitmentID      int64     `db:"commitment_id"`
	CommitmentCode    string    `db:"commitment_code"`
	ExpenseNatureCode string    `db:"expense_nature_code"`
	ExpenseCategory   string    `db:"expense_category"`
	ExpenseGroup      string    `db:"expense_group"`
	ExpenseElement    string    `db:"expense_element"`
	Description       string    `db:"description"`
	Quantity          float64   `db:"quantity"`
	Sequential        int16     `db:"sequential"` // Mapped from smallint
	UnitPrice         float64   `db:"unit_price"`
	CurrentValue      float64   `db:"current_value"`
	CurrentPrice      float64   `db:"current_price"`
	TotalPrice        float64   `db:"total_price"`
	InsertedAt        time.Time `db:"inserted_at"`
	UpdatedAt         time.Time `db:"updated_at"`
}

// CommitmentItemsHistory represents the 'commitment_items_history' table.
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
