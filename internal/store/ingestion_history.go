package store

import (
	"context"
	"log"

	"github.com/jmoiron/sqlx"
)

type IngestionHistoryStore struct {
	db *sqlx.DB
}

var (
	ScopeTypeManagingUnit = "Unidade Gestora"
	ScopeTypeManagement   = "Gestão"
)

var (
	TriggerTypeManual    = "manual"
	TriggerTypeScheduled = "scheduled"
)

var (
	StatusSuccess = "success"
	StatusFailure = "failure"
	StatusPartial = "partial"
)

func (ih *IngestionHistoryStore) InsertIngestionHistory(ctx context.Context, history *IngestionHistory) error {
	query := `INSERT INTO ingestion_history (
		reference_date,
		source_file,
		trigger_type,
		scope_type,
		status,
		processed_codes
	) VALUES (
		:reference_date,
		:source_file,
		:trigger_type,
		:scope_type,
		:status,
		:processed_codes
	) RETURNING id, processed_at`

	// Use NamedQuery to get the RETURNING values if needed,
	// or just NamedExec if we don't care about the generated fields in the struct.
	// Since we might want the ID and ProcessedAt back:
	rows, err := ih.db.NamedQuery(query, history)
	if err != nil {
		return err
	}
	defer rows.Close()

	if rows.Next() {
		err = rows.Scan(&history.ID, &history.ProcessedAt)
		if err != nil {
			return err
		}
	}

	log.Printf("Ingestion history recorded with ID: %d", history.ID)
	return nil
}
