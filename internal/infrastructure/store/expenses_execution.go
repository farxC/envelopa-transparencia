package store

import (
	"context"
	"fmt"

	"github.com/farxc/envelopa-transparencia/internal/domain/model"
	"github.com/farxc/envelopa-transparencia/internal/domain/service"
	"github.com/lib/pq"
)

type ExpensesExecutionStore struct {
	db GenericQueryer
}

func (s *ExpensesExecutionStore) InsertExpenseExecution(ctx context.Context, execution *model.ExpenseExecution) error {
	query := `
		INSERT INTO expenses_execution (
			year_and_month,
			superior_organ_code,
			superior_organ_name,
			subordinated_organ_code,
			subordinated_organ_name,
			management_unit_code,
			management_unit_name,
			management_code,
			management_name,
			action_code,
			action_name,
			budget_plan_code,
			budget_plan_name,
			federative_unit,
			municipality,
			author_amendament_code,
			author_amendament_name,
			economic_category_code,
			economic_category_name,
			expense_group_code,
			expense_group_name,
			expense_category_code,
			expense_category_name,
			expense_modality_code,
			expense_modality_name,
			committed_value_brl,
			liquidated_value_brl,
			paid_value_brl,
			registered_payables_amount_brl,
			canceled_payables_amount_brl,
			paid_payables_amount_brl,
			inserted_at,
			updated_at
		) VALUES (
			:year_and_month,
			:superior_organ_code,
			:superior_organ_name,
			:subordinated_organ_code,
			:subordinated_organ_name,
			:management_unit_code,
			:management_unit_name,
			:management_code,
			:management_name,
			:action_code,
			:action_name,
			:budget_plan_code,
			:budget_plan_name,
			:federative_unit,
			:municipality,
			:author_amendament_code,
			:author_amendament_name,
			:economic_category_code,
			:economic_category_name,
			:expense_group_code,
			:expense_group_name,
			:expense_category_code,
			:expense_category_name,
			:expense_modality_code,
			:expense_modality_name,
			:committed_value_brl,
			:liquidated_value_brl,
			:paid_value_brl,
			:registered_payables_amount_brl,
			:canceled_payables_amount_brl,
			:paid_payables_amount_brl,
			:inserted_at,
			:updated_at
		)
		ON CONFLICT (
			year_and_month,
			management_unit_code,
			management_code,
			action_code,
			budget_plan_code,
			expense_group_code,
			expense_category_code,
			expense_modality_code,
			federative_unit,
			municipality,
			author_amendament_code
		) DO UPDATE SET
			committed_value_brl            = EXCLUDED.committed_value_brl,
			liquidated_value_brl           = EXCLUDED.liquidated_value_brl,
			paid_value_brl                 = EXCLUDED.paid_value_brl,
			registered_payables_amount_brl = EXCLUDED.registered_payables_amount_brl,
			canceled_payables_amount_brl   = EXCLUDED.canceled_payables_amount_brl,
			paid_payables_amount_brl       = EXCLUDED.paid_payables_amount_brl,
			updated_at                     = EXCLUDED.updated_at
	`
	_, err := s.db.NamedExec(query, execution)
	fmt.Printf("Inserted expense execution for unit %d (%s): %v\n", execution.ManagementUnitCode, execution.ManagementUnitName, err)
	return err
}

func (s *ExpensesExecutionStore) GetBudgetExecution(ctx context.Context, e service.ExpensesFilter) ([]service.BudgetExecutionRow, error) {
	whereClause := "WHERE management_code = $1"
	args := []interface{}{e.ManagementCode}
	argIndex := 2

	if len(e.ManagementUnitCodes) > 0 {
		whereClause += fmt.Sprintf(" AND management_unit_code = ANY($%d)", argIndex)
		args = append(args, pq.Array(e.ManagementUnitCodes))
		argIndex++
	}

	if !e.StartDate.IsZero() && !e.EndDate.IsZero() {
		whereClause += fmt.Sprintf(" AND year_and_month >= $%d AND year_and_month <= $%d", argIndex, argIndex+1)
		args = append(args, e.StartDate.Format("2006-01"), e.EndDate.Format("2006-01"))
		argIndex += 2
	}

	query := fmt.Sprintf(`
		SELECT
			year_and_month, superior_organ_code, superior_organ_name,
			subordinated_organ_code, subordinated_organ_name,
			management_unit_code, management_unit_name,
			management_code, management_name,
			action_code, action_name,
			budget_plan_code, budget_plan_name,
			federative_unit, municipality,
			author_amendament_code, author_amendament_name,
			economic_category_code, economic_category_name,
			expense_group_code, expense_group_name,
			expense_category_code, expense_category_name,
			expense_modality_code, expense_modality_name,
			committed_value_brl, liquidated_value_brl, paid_value_brl,
			registered_payables_amount_brl, canceled_payables_amount_brl, paid_payables_amount_brl
		FROM expenses_execution
		%s
		ORDER BY year_and_month DESC, management_unit_code;
	`, whereClause)

	rows := make([]service.BudgetExecutionRow, 0)
	err := s.db.SelectContext(ctx, &rows, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query budget execution: %w", err)
	}
	return rows, nil
}
