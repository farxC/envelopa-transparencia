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
	}

	Expenses interface {
		SearchConsolidatedExpensesByExpensesNature(ctx context.Context, e ExpensesFilter) (map[string]float64, error)
		FilterExpensesTable(ctx context.Context, e ExpensesFilter) (ExpensesTableResult, error)
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
