package repository

import (
	"context"
	"time"

	"github.com/farxc/envelopa-transparencia/internal/domain/model"
	"github.com/farxc/envelopa-transparencia/internal/domain/service"
)

type CommitmentInterface interface {
	InsertCommitment(ctx context.Context, commitment *model.Commitment) error
	InsertCommitmentItem(ctx context.Context, item *model.CommitmentItem) error
	InsertCommitmentItemHistory(ctx context.Context, history *model.CommitmentItemsHistory) error
	GetCommitmentInformation(ctx context.Context, filter service.GetCommitmentInformationFilter) ([]service.CommitmentInformation, error)
}

type LiquidationInterface interface {
	InsertLiquidation(ctx context.Context, liquidation *model.Liquidation) error
	InsertLiquidationImpactedCommitment(ctx context.Context, lic *model.LiquidationImpactedCommitment) error
}

type PaymentInterface interface {
	InsertPayment(ctx context.Context, payment *model.Payment) error
	InsertPaymentImpactedCommitment(ctx context.Context, pic *model.PaymentImpactedCommitment) error
}

type IngestionHistoryInterface interface {
	InsertIngestionHistory(ctx context.Context, history *model.IngestionHistory) error
	GetLatest(ctx context.Context, limit int) ([]model.IngestionHistory, error)
	UpdateIngestionStatus(ctx context.Context, id int64, status string) error
	GetHistoryInRange(ctx context.Context, startDate, endDate time.Time, codes []int64) ([]model.IngestionHistory, error)
}

type ExpensesInterface interface {
	GetBudgetExecutionReport(ctx context.Context, e service.ExpensesFilter) (service.BudgetExecutionReportByUnit, error)
	GetBudgetExecutionSummary(ctx context.Context, e service.ExpensesFilter) (service.SummaryByUnits, error)
	GetBudgetExecutionSummaryByManagement(ctx context.Context, e service.ExpensesFilter) (service.GlobalSummary, error)
	GetTopFavored(ctx context.Context, e service.ExpensesFilter, limit int) ([]service.TopFavored, error)
}
