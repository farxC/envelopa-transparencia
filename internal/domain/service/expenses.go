package service

import (
	"time"
)

type ScopeType string

const (
	ByManagementUnit ScopeType = "management_unit"
	ByManagement     ScopeType = "management"
)

type ExpensesTableSummaryRow struct {
	ManagementUnitCode    string  `db:"management_unit_code" json:"management_unit_code"`
	CommittedAmount       float64 `db:"committed_amount" json:"committed_amount"`
	LiquidatedAmount      float64 `db:"liquidated_amount" json:"liquidated_amount"`
	PaidAmount            float64 `db:"paid_amount" json:"paid_amount"`
	BalanceToLiquidate    float64 `db:"balance_to_liquidate" json:"balance_to_liquidate"`
	BalanceToPayProcessed float64 `db:"balance_to_pay_processed" json:"balance_to_pay_processed"`
	ExecutionPercentage   float64 `db:"execution_percentage" json:"execution_percentage"`
}

type SummaryByUnits struct {
	Units []ExpensesTableSummaryRow `json:"units"`
}

type GlobalSummary struct {
	CommittedAmount       float64 `json:"committed_amount" db:"committed_amount"`
	LiquidatedAmount      float64 `json:"liquidated_amount" db:"liquidated_amount"`
	PaidAmount            float64 `json:"paid_amount" db:"paid_amount"`
	BalanceToLiquidate    float64 `json:"balance_to_liquidate" db:"balance_to_liquidate"`
	BalanceToPayProcessed float64 `json:"balance_to_pay_processed" db:"balance_to_pay_processed"`
	ExecutionPercentage   float64 `json:"execution_percentage" db:"execution_percentage"`
}

type TopFavored struct {
	FavoredCode    string  `db:"favored_code" json:"favored_code"`
	FavoredName    string  `db:"favored_name" json:"favored_name"`
	TotalPaidValue float64 `db:"total_paid_value" json:"total_paid_value"`
	PaymentsCount  int     `db:"payments_count" json:"payments_count"`
}

type ExpensesByCategory struct {
	CategoryCode   int16   `db:"expense_category_code" json:"category_code"`
	CategoryName   string  `db:"expense_category" json:"category_name"`
	TotalPaidValue float64 `db:"total_paid_value" json:"total_paid_value"`
}

type BudgetExecutionReport struct {
	ExpenseNature        string  `db:"expense_nature" json:"expense_nature"`
	Subitem              string  `db:"subitem" json:"subitem"`
	TransactionCount     int     `db:"transaction_count" json:"transaction_count"`
	TotalCommittedValue  float64 `db:"total_committed_value" json:"total_committed_value"`
	TotalLiquidatedValue float64 `db:"total_liquidated_value" json:"total_liquidated_value"`
	TotalPaidValue       float64 `db:"total_paid_value" json:"total_paid_value"`
	AveragePaymentValue  float64 `db:"average_payment_value" json:"average_payment_value"`
	PendingBalanceToPay  float64 `db:"pending_balance_to_pay" json:"pending_balance_to_pay"`
}

type BudgetExecutionReportByUnit map[string][]BudgetExecutionReport

type ExpensesFilter struct {
	ManagementCode      int
	ManagementUnitCodes []int
	StartDate           time.Time
	EndDate             time.Time
}
