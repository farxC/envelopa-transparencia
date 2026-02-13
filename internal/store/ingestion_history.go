package store

import (
	"context"
	"fmt"
	"log"

	"github.com/jmoiron/sqlx"
)

type IngestionHistoryStore struct {
	db *sqlx.DB
}

var (
	ScopeTypeManagingUnit = "MANAGEMENT_UNIT"
	ScopeTypeManagement   = "MANAGEMENT"
)

var (
	TriggerTypeManual    = "MANUAL"
	TriggerTypeScheduled = "SCHEDULED"
)

var (
	StatusSuccess    = "SUCCESS"
	StatusFailure    = "FAILURE"
	StatusPartial    = "PARTIAL"
	StatusInProgress = "IN_PROGRESS"
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

func (ih *IngestionHistoryStore) GetLatest(ctx context.Context, limit int) ([]IngestionHistory, error) {
	query := `
		SELECT id, processed_at, reference_date, source_file, trigger_type, scope_type, status, processed_codes
		FROM ingestion_history
		ORDER BY processed_at DESC
		LIMIT $1
	`
	var history []IngestionHistory
	err := ih.db.SelectContext(ctx, &history, query, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to get latest ingestion history: %w", err)
	}
	return history, nil
}

func (ih *IngestionHistoryStore) UpdateIngestionStatus(ctx context.Context, id int64, status string) error {
	query := `UPDATE ingestion_history SET status = $1 WHERE id = $2`
	_, err := ih.db.ExecContext(ctx, query, status, id)
	if err != nil {
		return fmt.Errorf("failed to update ingestion status: %w", err)
	}
	return nil
}
