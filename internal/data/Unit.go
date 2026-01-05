package data

import (
	"context"
	"database/sql"
)

type UnitStore struct {
	db *sql.DB
}

func (s *UnitStore) Get(ctx context.Context, ugs []string) error {
	return nil
}
