package repository

import (
	"context"

	"github.com/farxc/envelopa-transparencia/internal/domain/model"
	"github.com/farxc/envelopa-transparencia/internal/domain/service"
)

type ExpensesExecutionInterface interface {
	InsertExpenseExecution(ctx context.Context, execution *model.ExpenseExecution) error
	GetBudgetExecution(ctx context.Context, filter service.ExpensesFilter) ([]service.BudgetExecutionRow, error)
}
