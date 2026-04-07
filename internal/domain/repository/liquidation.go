package repository

import (
	"context"

	"github.com/farxc/envelopa-transparencia/internal/domain/model"
)

type LiquidationInterface interface {
	InsertLiquidation(ctx context.Context, liquidation *model.Liquidation) error
	InsertLiquidationImpactedCommitment(ctx context.Context, lic *model.LiquidationImpactedCommitment) error
}
