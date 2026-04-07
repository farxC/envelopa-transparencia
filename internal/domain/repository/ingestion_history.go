package repository

import (
	"context"
	"time"

	"github.com/farxc/envelopa-transparencia/internal/domain/model"
)

type IngestionHistoryInterface interface {
	InsertIngestionHistory(ctx context.Context, history *model.IngestionHistory) error
	GetLatest(ctx context.Context, limit int) ([]model.IngestionHistory, error)
	UpdateIngestionStatus(ctx context.Context, id int64, status string) error
	GetHistoryInRange(ctx context.Context, startDate, endDate time.Time, codes []int64) ([]model.IngestionHistory, error)
}
