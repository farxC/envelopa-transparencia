package store

import (
	"context"
	"fmt"

	"github.com/farxc/envelopa-transparencia/internal/domain/service"
	"github.com/lib/pq"
)

type ExpensesStore struct {
	db GenericQueryer
}

/*
This store is responsible for querying the database to generate the expenses summary and report based on the provided filters (date range and management unit codes).
The GetBudgetExecutionReport method retrieves detailed information about expenses by nature for each management unit, while the GetBudgetExecutionSummary method provides a consolidated view of committed, liquidated, and paid amounts, along with execution percentages.
*/
func (es *ExpensesStore) GetBudgetExecutionReport(ctx context.Context, e service.ExpensesFilter) (service.BudgetExecutionReportByUnit, error) {
	whereClause := "WHERE c.management_code = $1"
	args := []interface{}{e.ManagementCode}
	argIndex := 2

	// Optional management unit codes filter
	if len(e.ManagementUnitCodes) > 0 {
		whereClause += fmt.Sprintf(" AND c.management_unit_code = ANY($%d)", argIndex)
		args = append(args, pq.Array(e.ManagementUnitCodes))
		argIndex++
	}

	// Optional date range filter
	if !e.StartDate.IsZero() && !e.EndDate.IsZero() {
		whereClause += fmt.Sprintf(" AND c.emission_date BETWEEN $%d AND $%d", argIndex, argIndex+1)
		args = append(args, e.StartDate, e.EndDate)
		argIndex += 2
	}

	query := fmt.Sprintf(`
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
	%s
		AND pic.expense_nature_code_complete IS NOT NULL
	GROUP BY 
		c.management_unit_code,
		c.management_unit_name,
		pic.expense_nature_code_complete, 
		pic.subitem 
	ORDER BY 
		c.management_unit_code,
		total_paid_value DESC;
	`, whereClause)

	rows, err := es.db.QueryxContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query expenses by nature: %w", err)
	}
	defer rows.Close()

	result := make(service.BudgetExecutionReportByUnit)

	for rows.Next() {
		rowResult := &service.BudgetExecutionReport{}
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

func (es *ExpensesStore) GetBudgetExecutionSummary(ctx context.Context, e service.ExpensesFilter) (service.SummaryByUnits, error) {
	whereClauseCommitments := "WHERE c.management_code = $1"
	whereClauseLiquidations := "WHERE l.management_code = $1"
	whereClausePayments := "WHERE p.management_code = $1"
	args := []interface{}{e.ManagementCode}
	argIndex := 2

	// Optional management unit codes filter
	if len(e.ManagementUnitCodes) > 0 {
		whereClauseCommitments += fmt.Sprintf(" AND c.management_unit_code = ANY($%d)", argIndex)
		whereClauseLiquidations += fmt.Sprintf(" AND l.management_unit_code = ANY($%d)", argIndex)
		whereClausePayments += fmt.Sprintf(" AND p.management_unit_code = ANY($%d)", argIndex)
		args = append(args, pq.Array(e.ManagementUnitCodes))
		argIndex++
	}

	// Optional date range filter
	if !e.StartDate.IsZero() && !e.EndDate.IsZero() {
		whereClauseCommitments += fmt.Sprintf(" AND c.emission_date BETWEEN $%d AND $%d", argIndex, argIndex+1)
		whereClauseLiquidations += fmt.Sprintf(" AND l.liquidation_emission_date BETWEEN $%d AND $%d", argIndex, argIndex+1)
		whereClausePayments += fmt.Sprintf(" AND p.payment_emission_date BETWEEN $%d AND $%d", argIndex, argIndex+1)
		args = append(args, e.StartDate, e.EndDate)
		argIndex += 2
	}

	query := fmt.Sprintf(`
	WITH TotalCommitted AS (
		SELECT 
			management_unit_code,
			COALESCE(SUM(ci.current_value), 0) AS committed_amount
		FROM 
			commitments c 
		LEFT JOIN 
			commitment_items ci ON c.id = ci.commitment_id 
		%s
		GROUP BY 
			management_unit_code
	),

	TotalLiquidated AS (
		SELECT 
			management_unit_code,
			COALESCE(SUM(lic.liquidated_value_brl), 0) AS liquidated_amount
		FROM 
			liquidations l
		LEFT JOIN 
			liquidation_impacted_commitments lic ON l.liquidation_code = lic.liquidation_code
		%s
		GROUP BY 
			management_unit_code
	),

	TotalPaid AS (
		SELECT 
			management_unit_code,
			COALESCE(SUM(pic.paid_value_brl), 0) AS paid_amount
		FROM 
			payments p
		LEFT JOIN 
			payment_impacted_commitments pic ON p.payment_code = pic.payment_code 
		%s
		GROUP BY 
			management_unit_code
	)

	SELECT 
		c.management_unit_code AS management_unit_code,
		COALESCE(c.committed_amount, 0) AS committed_amount,
		COALESCE(l.liquidated_amount, 0) AS liquidated_amount,
		COALESCE(p.paid_amount, 0) AS paid_amount,
		(COALESCE(c.committed_amount, 0) - COALESCE(l.liquidated_amount, 0)) AS balance_to_liquidate,
		(COALESCE(l.liquidated_amount, 0) - COALESCE(p.paid_amount, 0)) AS balance_to_pay_processed,
		CASE 
			WHEN COALESCE(c.committed_amount, 0) > 0 THEN ROUND((COALESCE(p.paid_amount, 0) / c.committed_amount) * 100, 2)
			ELSE 0 
		END AS execution_percentage
	FROM 
		TotalCommitted c
	FULL OUTER JOIN TotalLiquidated l ON c.management_unit_code = l.management_unit_code
	FULL OUTER JOIN TotalPaid p ON COALESCE(c.management_unit_code, l.management_unit_code) = p.management_unit_code;
	`, whereClauseCommitments, whereClauseLiquidations, whereClausePayments)

	var rows []service.ExpensesTableSummaryRow
	err := es.db.SelectContext(ctx, &rows, query, args...)
	if err != nil {
		return service.SummaryByUnits{}, fmt.Errorf("failed to query consolidated expenses: %w", err)
	}

	return service.SummaryByUnits{
		Units: rows,
	}, nil
}

func (es *ExpensesStore) GetBudgetExecutionSummaryByManagement(ctx context.Context, e service.ExpensesFilter) (service.GlobalSummary, error) {
	whereClauseCommitments := "WHERE c.management_code = $1"
	whereClauseLiquidations := "WHERE l.management_code = $1"
	whereClausePayments := "WHERE p.management_code = $1"
	args := []interface{}{e.ManagementCode}
	argIndex := 2

	// Optional date range filter
	if !e.StartDate.IsZero() && !e.EndDate.IsZero() {
		whereClauseCommitments += fmt.Sprintf(" AND c.emission_date BETWEEN $%d AND $%d", argIndex, argIndex+1)
		whereClauseLiquidations += fmt.Sprintf(" AND l.liquidation_emission_date BETWEEN $%d AND $%d", argIndex, argIndex+1)
		whereClausePayments += fmt.Sprintf(" AND p.payment_emission_date BETWEEN $%d AND $%d", argIndex, argIndex+1)
		args = append(args, e.StartDate, e.EndDate)
		argIndex += 2
	}

	query := fmt.Sprintf(`
	WITH Totals AS (
		SELECT 
			COALESCE(SUM(ci.current_value), 0) AS committed_amount,
			(SELECT COALESCE(SUM(lic.liquidated_value_brl), 0) FROM liquidations
				l JOIN
				liquidation_impacted_commitments lic 
				ON l.liquidation_code = lic.liquidation_code 
				%s)
				AS liquidated_amount,
			(SELECT COALESCE(SUM(pic.paid_value_brl), 0) 
				FROM payments p 
				JOIN payment_impacted_commitments pic 
				ON p.payment_code = pic.payment_code 
				%s) 
			AS paid_amount
		FROM 
			commitments c 
		JOIN 
			commitment_items ci ON c.id = ci.commitment_id 
		%s
	)
	SELECT 
		committed_amount,
		liquidated_amount,
		paid_amount,
		(committed_amount - liquidated_amount) AS balance_to_liquidate,
		(liquidated_amount - paid_amount) AS balance_to_pay_processed,
		CASE 
			WHEN committed_amount > 0 THEN ROUND((paid_amount / committed_amount) * 100, 2)
			ELSE 0 
		END AS execution_percentage
	FROM 
		Totals;
	`, whereClauseLiquidations, whereClausePayments, whereClauseCommitments)

	var result service.GlobalSummary
	err := es.db.GetContext(ctx, &result, query, args...)
	if err != nil {
		return service.GlobalSummary{}, fmt.Errorf("failed to query global expenses summary: %w", err)
	}

	return result, nil
}

func (es *ExpensesStore) GetTopFavored(ctx context.Context, e service.ExpensesFilter, limit int) ([]service.TopFavored, error) {
	whereClause := "WHERE p.management_code = $1"
	args := []interface{}{e.ManagementCode}
	argIndex := 2

	// Optional management unit codes filter
	if len(e.ManagementUnitCodes) > 0 {
		whereClause += fmt.Sprintf(" AND p.management_unit_code = ANY($%d)", argIndex)
		args = append(args, pq.Array(e.ManagementUnitCodes))
		argIndex++
	}

	// Optional date range filter
	if !e.StartDate.IsZero() && !e.EndDate.IsZero() {
		whereClause += fmt.Sprintf(" AND p.payment_emission_date BETWEEN $%d AND $%d", argIndex, argIndex+1)
		args = append(args, e.StartDate, e.EndDate)
		argIndex += 2
	}

	query := fmt.Sprintf(`
	SELECT 
		p.favored_code,
		p.favored_name,
		SUM(pic.paid_value_brl) AS total_paid_value,
		COUNT(p.id) AS payments_count
	FROM 
		payments p
	JOIN 
		payment_impacted_commitments pic ON p.payment_code = pic.payment_code
	%s
	GROUP BY 
		p.favored_code, p.favored_name
	ORDER BY 
		total_paid_value DESC
	LIMIT $%d;
	`, whereClause, argIndex)

	args = append(args, limit)

	var result []service.TopFavored
	err := es.db.SelectContext(ctx, &result, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query top favored: %w", err)
	}

	return result, nil
}
