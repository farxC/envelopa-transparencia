package repository

import (
	"context"

	"github.com/farxc/envelopa-transparencia/internal/domain/service"
)

type ExpensesInterface interface {
	GetBudgetExecutionReport(ctx context.Context, e service.ExpensesFilter) (service.BudgetExecutionReportByUnit, error)
	GetBudgetExecutionSummary(ctx context.Context, e service.ExpensesFilter) (service.SummaryByUnits, error)
	GetBudgetExecutionSummaryByManagement(ctx context.Context, e service.ExpensesFilter) (service.GlobalSummary, error)
	GetTopFavored(ctx context.Context, e service.ExpensesFilter, limit int) ([]service.TopFavored, error)
}
