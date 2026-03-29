package application

import (
	"context"
	"time"

	"github.com/farxc/envelopa-transparencia/internal/domain/model"
)

// Pipeline defines the contract that any ETL extraction type must satisfy
// to be driven by the generic Orchestrator.
//
// J is the job type specific to this pipeline (e.g. model.ExpensesDailyJob).
//
// The orchestrator owns the job lifecycle (IN_PROGRESS → SUCCESS/FAILURE/SKIP).
// The pipeline owns everything domain-specific: what to download, how to extract,
// how to load, and how to interpret errors.
type Pipeline[J any] interface {
	// Execute runs the full ETL for a single job: download → extract → transform → load.
	// It must NOT interact with IngestionHistory — that is the orchestrator's responsibility.
	Execute(ctx context.Context, job J) error

	BuildHistoryRecord(job J) *model.IngestionHistory

	ShouldSkip(err error, job J) bool

	StatusKey(job J) string

	HistoryKey(h model.IngestionHistory) string

	HistoryRange(startDate, endDate time.Time) (time.Time, time.Time)
}
