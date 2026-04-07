package repository

import (
	"context"

	"github.com/farxc/envelopa-transparencia/internal/domain/model"
	"github.com/farxc/envelopa-transparencia/internal/domain/service"
)

type CommitmentInterface interface {
	InsertCommitment(ctx context.Context, commitment *model.Commitment) error
	InsertCommitmentItem(ctx context.Context, item *model.CommitmentItem) error
	InsertCommitmentItemHistory(ctx context.Context, history *model.CommitmentItemsHistory) error
	GetCommitmentInformation(ctx context.Context, filter service.GetCommitmentInformationFilter) ([]service.CommitmentInformation, error)
}
