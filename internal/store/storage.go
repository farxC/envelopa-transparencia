package store

import (
	"context"

	"github.com/jmoiron/sqlx"
)

type Storage struct {
	Commitment interface {
		InsertCommitment(ctx context.Context, commitment *Commitment) error
	}

	CommitmentItem interface {
		InsertCommitmentItem(ctx context.Context, item *CommitmentItem) error
	}

	CommitmentItemHistory interface {
		InsertCommitmentItemHistory(ctx context.Context, history *CommitmentItemsHistory) error
	}

	Liquidation interface {
		InsertLiquidation(ctx context.Context, liquidation *Liquidation) error
	}

	LiquidationImpactedCommitment interface {
		InsertLiquidationImpactedCommitment(ctx context.Context, lic *LiquidationImpactedCommitment) error
	}

	Payment interface {
		InsertPayment(ctx context.Context, payment *Payment) error
	}

	PaymentImpactedCommitment interface {
		InsertPaymentImpactedCommitment(ctx context.Context, pic *PaymentImpactedCommitment) error
	}
}

func NewStorage(db *sqlx.DB) *Storage {
	return &Storage{
		Commitment: &CommitmentStore{db: db},
	}

}
