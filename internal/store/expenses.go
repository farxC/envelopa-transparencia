package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"
)

type ExpensesStore struct {
	db *sqlx.DB
}

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
	Rows []ExpensesTableSummaryRow `json:"rows"`
}

type BudgetExecutionReport struct {
	ExpenseNatureCodeComplete string  `db:"expense_nature_code_complete" json:"expense_nature_code_complete"`
	Subitem                   string  `db:"subitem" json:"subitem"`
	TransactionsCount         int     `db:"transactions_count" json:"transactions_count"`
	TotalPaidValue            float64 `db:"total_paid_value" json:"total_paid_value"`
	AveragePaymentValue       float64 `db:"average_payment_value" json:"average_payment_value"`
	OutstandingValuePaid      float64 `db:"outstanding_value_paid" json:"outstanding_value_paid"`
}

type BudgetExecutionReportByUnit map[string][]BudgetExecutionReport

type ExpensesFilter struct {
	StartDate time.Time
	EndDate   time.Time
	Codes     string
}

/*
This store is responsible for querying the database to generate the expenses summary and report based on the provided filters (date range and management unit codes).
The GetBudgetExecutionReport method retrieves detailed information about expenses by nature for each management unit, while the GetBudgetExecutionSummary method provides a consolidated view of committed, liquidated, and paid amounts, along with execution percentages.
*/
func (es *ExpensesStore) GetBudgetExecutionReport(ctx context.Context, e ExpensesFilter) (BudgetExecutionReportByUnit, error) {

	query := `
	SELECT 
		c.management_unit_code AS management_unit_code,
		pic.expense_nature_code_complete AS expense_nature_code_complete,
		pic.subitem,
		COUNT(pic.id) AS transactions_count,
		SUM(pic.paid_value_brl) AS total_paid_value,
		ROUND(AVG(pic.paid_value_brl), 2) AS average_payment_value,
		SUM(pic.outstanding_value_paid_brl) AS outstanding_value_paid
	FROM 
		commitments c 
	JOIN 
		payment_impacted_commitments pic ON pic.commitment_code = c.commitment_code 
	WHERE 
		c.emission_date BETWEEN $1 AND $2
		AND c.management_unit_code IN ($3) 
		AND pic.expense_nature_code_complete IS NOT NULL 
		AND pic.expense_nature_code_complete != ''
	GROUP BY 
		c.management_unit_code,
		c.management_unit_name,
		pic.expense_nature_code_complete, 
		pic.subitem 
	ORDER BY 
		c.management_unit_code,
		total_paid_value DESC;
	`
	rows, err := es.db.QueryxContext(ctx, query, e.StartDate, e.EndDate, e.Codes)
	if err != nil {
		return nil, fmt.Errorf("failed to query expenses by nature: %w", err)
	}
	defer rows.Close()

	result := make(BudgetExecutionReportByUnit)

	for rows.Next() {
		rowResult := &BudgetExecutionReport{}
		unitGroup := ""

		err := rows.Scan(&unitGroup, &rowResult.ExpenseNatureCodeComplete, &rowResult.Subitem, &rowResult.TransactionsCount, &rowResult.TotalPaidValue, &rowResult.AveragePaymentValue, &rowResult.OutstandingValuePaid)
		if err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}

		result[unitGroup] = append(result[unitGroup], *rowResult)
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("rows iteration error: %w", err)
	}

	return result, nil
}

func (es *ExpensesStore) GetBudgetExecutionSummary(ctx context.Context, e ExpensesFilter) (SummaryByUnits, error) {

	query := `
	WITH TotalCommitted AS (
		SELECT 
			management_unit_code as management_unit_code,
			COALESCE(SUM(ci.current_value), 0) AS committed_amount
		FROM 
			commitments c 
		LEFT JOIN 
			commitment_items ci ON c.id = ci.commitment_id 
		WHERE 
			c.emission_date BETWEEN $1 AND $2
		AND 
			c.management_unit_code IN ($3)
		GROUP BY 
			management_unit_code
	),

	TotalLiquidated AS (
		SELECT 
			management_unit_code as management_unit_code,
			COALESCE(SUM(lic.liquidated_value_brl), 0) AS liquidated_amount
		FROM 
			liquidations l
		LEFT JOIN 
			liquidation_impacted_commitments lic ON l.liquidation_code = lic.liquidation_code
		WHERE 
			l.liquidation_emission_date BETWEEN $1 AND $2
		AND 
			l.management_unit_code IN ($3)
		GROUP BY 
			management_unit_code
	),

	TotalPaid AS (
		SELECT 
			management_unit_code as management_unit_code,
			COALESCE(SUM(pic.paid_value_brl), 0) AS paid_amount
		FROM 
			payments p
		LEFT JOIN 
			payment_impacted_commitments pic ON p.payment_code = pic.payment_code 
		WHERE 
			p.payment_emission_date BETWEEN $1 AND $2
		AND 
			p.management_unit_code IN ($3)
		GROUP BY 
			management_unit_code
	)

	-- 4. Consolidação
	SELECT 
		c.management_unit_code,
		c.committed_amount,
		l.liquidated_amount,
		p.paid_amount,
		(c.committed_amount - l.liquidated_amount) AS balance_to_liquidate,
		(l.liquidated_amount - p.paid_amount) AS balance_to_pay_processed,
		CASE 
			WHEN c.committed_amount > 0 THEN ROUND((p.paid_amount / c.committed_amount) * 100, 2)
			ELSE 0 
		END AS execution_percentage
	FROM 
		TotalCommitted c
		CROSS JOIN TotalLiquidated l
		CROSS JOIN TotalPaid p;
	`

	// 33000000

	var result ExpensesTableSummaryRow

	err := es.db.GetContext(ctx, &result, query, e.StartDate, e.EndDate, e.Codes)
	if err != nil {
		if err == sql.ErrNoRows {
			return SummaryByUnits{
				Rows: []ExpensesTableSummaryRow{},
			}, nil
		}
		return SummaryByUnits{}, fmt.Errorf("failed to query consolidated expenses: %w", err)
	}

	return SummaryByUnits{
		Rows: []ExpensesTableSummaryRow{result},
	}, nil
}
