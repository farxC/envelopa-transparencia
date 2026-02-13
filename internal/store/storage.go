package store

import (
	"context"

	"github.com/jmoiron/sqlx"
)

type ExpenseRecord struct {
}
type Storage struct {
	Commitment interface {
		InsertCommitment(ctx context.Context, commitment *Commitment) error
		InsertCommitmentItem(ctx context.Context, item *CommitmentItem) error
		InsertCommitmentItemHistory(ctx context.Context, history *CommitmentItemsHistory) error
		GetCommitmentInformation(ctx context.Context, filter GetCommitmentInformationFilter) ([]CommitmentInformation, error)
	}

	Liquidation interface {
		InsertLiquidation(ctx context.Context, liquidation *Liquidation) error
		InsertLiquidationImpactedCommitment(ctx context.Context, lic *LiquidationImpactedCommitment) error
	}

	Payment interface {
		InsertPayment(ctx context.Context, payment *Payment) error
		InsertPaymentImpactedCommitment(ctx context.Context, pic *PaymentImpactedCommitment) error
	}

	IngestionHistory interface {
		InsertIngestionHistory(ctx context.Context, history *IngestionHistory) error
		GetLatest(ctx context.Context, limit int) ([]IngestionHistory, error)
		UpdateIngestionStatus(ctx context.Context, id int64, status string) error
	}

	Expenses interface {
		GetBudgetExecutionReport(ctx context.Context, e ExpensesFilter) (BudgetExecutionReportByUnit, error)
		GetBudgetExecutionSummary(ctx context.Context, e ExpensesFilter) (SummaryByUnits, error)
		GetGlobalBudgetExecutionSummary(ctx context.Context, e ExpensesFilter) (GlobalSummary, error)
		GetTopFavored(ctx context.Context, e ExpensesFilter, limit int) ([]TopFavored, error)
		GetExpensesByCategory(ctx context.Context, e ExpensesFilter) ([]ExpensesByCategory, error)
	}
}

func NewStorage(db *sqlx.DB) *Storage {
	return &Storage{
		Commitment:       &CommitmentStore{db: db},
		Liquidation:      &LiquidationStore{db: db},
		Payment:          &PaymentStore{db: db},
		IngestionHistory: &IngestionHistoryStore{db: db},
		Expenses:         &ExpensesStore{db: db},
	}

}
