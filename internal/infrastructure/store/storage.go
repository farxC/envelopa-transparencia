package store

import (
	"context"
	"database/sql"

	"github.com/farxc/envelopa-transparencia/internal/domain/repository"
	"github.com/jmoiron/sqlx"
)

type Storage struct {
	Commitment repository.CommitmentInterface

	Liquidation repository.LiquidationInterface

	Payment repository.PaymentInterface

	IngestionHistory repository.IngestionHistoryInterface

	Expenses repository.ExpensesInterface

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
