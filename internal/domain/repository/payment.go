package repository

import (
	"context"

	"github.com/farxc/envelopa-transparencia/internal/domain/model"
)

type PaymentInterface interface {
	InsertPayment(ctx context.Context, payment *model.Payment) error
	InsertPaymentImpactedCommitment(ctx context.Context, pic *model.PaymentImpactedCommitment) error
}
