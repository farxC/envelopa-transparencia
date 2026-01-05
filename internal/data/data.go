package data

import (
	"context"
	"database/sql"
)

type Unit interface {
	Get(ctx context.Context, ugs []string) error
}

type Storage struct {
	Unit
}

func NewStorage(db *sql.DB) Storage {
	return Storage{
		Unit: &UnitStore{db},
	}
}
