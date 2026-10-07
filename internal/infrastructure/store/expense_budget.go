package store

import (
	"context"
	"fmt"

	"github.com/farxc/envelopa-transparencia/internal/domain/model"
	"github.com/farxc/envelopa-transparencia/internal/domain/service"
	"github.com/lib/pq"
)

type ExpenseBudgetStore struct {
	db GenericQueryer
}

func (s *ExpenseBudgetStore) InsertExpenseBudget(ctx context.Context, b *model.ExpenseBudget) error {
	query := `
		INSERT INTO expense_budget (
			exercise,
			superior_organ_code,
			superior_organ_name,
			subordinate_agency_code,
			subordinate_agency_name,
			budgetary_unit_code,
			budgetary_unit_name,
			function_code,
			function_name,
			subfunction_code,
			subfunction_name,
			budget_program_code,
			budget_program_name,
			action_code,
			action_name,
			economic_category_code,
			economic_category,
			expense_group_code,
			expense_group_name,
			expense_element_code,
			expense_element_name,
			initial_budget,
			updated_budget,
			committed_budget,
			executed_budget,
			percent_executed_budget,
			inserted_at,
			updated_at
		) VALUES (
			:exercise,
			:superior_organ_code,
			:superior_organ_name,
			:subordinate_agency_code,
			:subordinate_agency_name,
			:budgetary_unit_code,
			:budgetary_unit_name,
			:function_code,
			:function_name,
			:subfunction_code,
			:subfunction_name,
			:budget_program_code,
			:budget_program_name,
			:action_code,
			:action_name,
			:economic_category_code,
			:economic_category,
			:expense_group_code,
			:expense_group_name,
			:expense_element_code,
			:expense_element_name,
			:initial_budget,
			:updated_budget,
			:committed_budget,
			:executed_budget,
			:percent_executed_budget,
			:inserted_at,
			:updated_at
		)
		ON CONFLICT (
			exercise,
			subordinate_agency_code,
			budgetary_unit_code,
			function_code,
			subfunction_code,
			budget_program_code,
			action_code,
			economic_category_code,
			expense_group_code,
			expense_element_code
		) DO UPDATE SET
			superior_organ_code     = EXCLUDED.superior_organ_code,
			superior_organ_name     = EXCLUDED.superior_organ_name,
			subordinate_agency_name = EXCLUDED.subordinate_agency_name,
			budgetary_unit_name     = EXCLUDED.budgetary_unit_name,
			function_name           = EXCLUDED.function_name,
			subfunction_name        = EXCLUDED.subfunction_name,
			budget_program_name     = EXCLUDED.budget_program_name,
			action_name             = EXCLUDED.action_name,
			economic_category       = EXCLUDED.economic_category,
			expense_group_name      = EXCLUDED.expense_group_name,
			expense_element_name    = EXCLUDED.expense_element_name,
			initial_budget          = EXCLUDED.initial_budget,
			updated_budget          = EXCLUDED.updated_budget,
			committed_budget        = EXCLUDED.committed_budget,
			executed_budget         = EXCLUDED.executed_budget,
			percent_executed_budget = EXCLUDED.percent_executed_budget,
			updated_at              = EXCLUDED.updated_at
	`
	_, err := s.db.NamedExec(query, b)
	return err
}

// DeleteExpenseBudget removes the rows of an exercise for the given
// subordinate agencies, so a reload drops rows that left the portal file.
func (s *ExpenseBudgetStore) DeleteExpenseBudget(ctx context.Context, exercise int, agencyCodes []int64) error {
	_, err := s.db.ExecContext(ctx,
		`DELETE FROM expense_budget WHERE exercise = $1 AND subordinate_agency_code = ANY($2)`,
		exercise, pq.Array(agencyCodes))
	return err
}

func (s *ExpenseBudgetStore) GetExpenseBudget(ctx context.Context, filter service.BudgetFilter) ([]service.BudgetRow, error) {
	whereClause := "WHERE subordinate_agency_code = ANY($1)"
	args := []interface{}{pq.Array(filter.SubordinateAgencyCodes)}
	argIndex := 2

	if filter.Exercise != 0 {
		whereClause += fmt.Sprintf(" AND exercise = $%d", argIndex)
		args = append(args, filter.Exercise)
		argIndex++
	}

	_ = argIndex
	query := fmt.Sprintf(`
		SELECT
			exercise,
			superior_organ_code, superior_organ_name,
			subordinate_agency_code, subordinate_agency_name,
			budgetary_unit_code, budgetary_unit_name,
			function_code, function_name,
			subfunction_code, subfunction_name,
			budget_program_code, budget_program_name,
			action_code, action_name,
			economic_category_code, economic_category,
			expense_group_code, expense_group_name,
			expense_element_code, expense_element_name,
			initial_budget, updated_budget, committed_budget,
			executed_budget, percent_executed_budget
		FROM expense_budget
		%s
		ORDER BY exercise DESC, action_code
	`, whereClause)

	rows := make([]service.BudgetRow, 0)
	err := s.db.SelectContext(ctx, &rows, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query expense budget: %w", err)
	}
	return rows, nil
}

func (s *ExpenseBudgetStore) GetExpenseBudgetSummary(ctx context.Context, filter service.BudgetFilter) ([]service.BudgetSummaryRow, error) {
	whereClause := "WHERE subordinate_agency_code = ANY($1)"
	args := []interface{}{pq.Array(filter.SubordinateAgencyCodes)}
	argIndex := 2

	if filter.Exercise != 0 {
		whereClause += fmt.Sprintf(" AND exercise = $%d", argIndex)
		args = append(args, filter.Exercise)
		argIndex++
	}

	_ = argIndex
	query := fmt.Sprintf(`
		SELECT
			action_code,
			MAX(action_name)          AS action_name,
			CONCAT(
				economic_category_code::text, '.',
				expense_group_code::text,     '.',
				LPAD(expense_element_code::text, 2, '0')
			)                         AS expense_nature,
			MAX(economic_category)    AS economic_category,
			MAX(expense_group_name)   AS expense_group_name,
			MAX(expense_element_name) AS expense_element_name,
			SUM(initial_budget)       AS initial_budget,
			SUM(updated_budget)       AS updated_budget,
			SUM(committed_budget)     AS committed_budget,
			SUM(executed_budget)      AS executed_budget,
			CASE
				WHEN SUM(updated_budget) > 0
				THEN ROUND((SUM(executed_budget) / SUM(updated_budget) * 100)::numeric, 4)
				ELSE 0
			END                       AS percent_executed_budget
		FROM expense_budget
		%s
		GROUP BY
			action_code,
			economic_category_code,
			expense_group_code,
			expense_element_code
		ORDER BY action_code, expense_nature
	`, whereClause)

	rows := make([]service.BudgetSummaryRow, 0)
	err := s.db.SelectContext(ctx, &rows, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query expense budget summary: %w", err)
	}
	return rows, nil
}

func (s *ExpenseBudgetStore) GetExpenseBudgetGlobalSummary(ctx context.Context, filter service.BudgetFilter) ([]service.BudgetGlobalSummaryRow, error) {
	whereClause := "WHERE subordinate_agency_code = ANY($1)"
	args := []interface{}{pq.Array(filter.SubordinateAgencyCodes)}
	argIndex := 2

	if filter.Exercise != 0 {
		whereClause += fmt.Sprintf(" AND exercise = $%d", argIndex)
		args = append(args, filter.Exercise)
		argIndex++
	}

	_ = argIndex
	query := fmt.Sprintf(`
		SELECT
			action_code,
			MAX(action_name)      AS action_name,
			SUM(initial_budget)   AS initial_budget,
			SUM(updated_budget)   AS updated_budget,
			SUM(committed_budget) AS committed_budget,
			SUM(executed_budget)  AS executed_budget,
			CASE
				WHEN SUM(updated_budget) > 0
				THEN ROUND((SUM(executed_budget) / SUM(updated_budget) * 100)::numeric, 4)
				ELSE 0
			END                   AS percent_executed_budget
		FROM expense_budget
		%s
		GROUP BY action_code
		ORDER BY action_code
	`, whereClause)

	rows := make([]service.BudgetGlobalSummaryRow, 0)
	err := s.db.SelectContext(ctx, &rows, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query expense budget global summary: %w", err)
	}
	return rows, nil
}
