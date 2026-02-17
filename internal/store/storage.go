package store

import (
	"context"
	"database/sql"
	"time"

	"github.com/jmoiron/sqlx"
)

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
		GetHistoryInRange(ctx context.Context, startDate, endDate time.Time, codes []int64) ([]IngestionHistory, error)
	}

	Expenses interface {
		GetBudgetExecutionReport(ctx context.Context, e ExpensesFilter) (BudgetExecutionReportByUnit, error)
		GetBudgetExecutionSummary(ctx context.Context, e ExpensesFilter) (SummaryByUnits, error)
		GetBudgetExecutionSummaryByManagement(ctx context.Context, e ExpensesFilter) (GlobalSummary, error)
		GetTopFavored(ctx context.Context, e ExpensesFilter, limit int) ([]TopFavored, error)
	}
	DB *sqlx.DB
}

// Defines an generic interface for group both *sqlx.Tx and *sqlx.Db
type GenericQueryer interface {
	NamedExec(query string, arg interface{}) (sql.Result, error)
	SelectContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error
	GetContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error
	QueryxContext(ctx context.Context, query string, args ...interface{}) (*sqlx.Rows, error)
	NamedQuery(query string, arg interface{}) (*sqlx.Rows, error)
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

func (s *Storage) WithTx(tx *sqlx.Tx) *Storage {
	return &Storage{
		Commitment:       &CommitmentStore{db: tx},
		Liquidation:      &LiquidationStore{db: tx},
		Payment:          &PaymentStore{db: tx},
		IngestionHistory: &IngestionHistoryStore{db: tx},
		Expenses:         &ExpensesStore{db: tx},
	}
}

func NewStorage(db *sqlx.DB) *Storage {
	return &Storage{
		Commitment:       &CommitmentStore{db: db},
		Liquidation:      &LiquidationStore{db: db},
		Payment:          &PaymentStore{db: db},
		IngestionHistory: &IngestionHistoryStore{db: db},
		Expenses:         &ExpensesStore{db: db},
		DB:               db,
	}
}
