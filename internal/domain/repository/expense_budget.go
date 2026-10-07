package repository

import (
	"context"

	"github.com/farxc/envelopa-transparencia/internal/domain/model"
	"github.com/farxc/envelopa-transparencia/internal/domain/service"
)

type ExpenseBudgetInterface interface {
	InsertExpenseBudget(ctx context.Context, b *model.ExpenseBudget) error
	DeleteExpenseBudget(ctx context.Context, exercise int, agencyCodes []int64) error
	GetExpenseBudget(ctx context.Context, filter service.BudgetFilter) ([]service.BudgetRow, error)
	GetExpenseBudgetSummary(ctx context.Context, filter service.BudgetFilter) ([]service.BudgetSummaryRow, error)
	GetExpenseBudgetGlobalSummary(ctx context.Context, filter service.BudgetFilter) ([]service.BudgetGlobalSummaryRow, error)
}
