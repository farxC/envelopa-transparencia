package store

import (
	"context"
	"log"

	"github.com/jmoiron/sqlx"
)

type CommitmentStore struct {
	db *sqlx.DB
}

func (cs *CommitmentStore) InsertCommitment(ctx context.Context, commitment *Commitment) error {

	query := `
		INSERT INTO payments (
			payment_code,
			payment_code_resumed,
			payment_emission_date,
			document_code_type,
			document_type,
			favored_code,
			favored_name,
			extra_budgetary,
			process,
			original_payment_value,
			converted_payment_value,
			conversion_used_value
		) VALUES (
			:payment_code,
			:payment_code_resumed,
			:payment_emission_date,
			:document_code_type,
			:document_type,
			:favored_code,
			:favored_name,
			:extra_budgetary,
			:process,
			:original_payment_value,
			:converted_payment_value,
			:conversion_used_value
		)`

	result, err := cs.db.NamedExec(query, commitment)

	if err != nil {
		return err
	}
	rowsAffected, _ := result.RowsAffected()
	log.Printf("Insertion successful, %d rows affected.", rowsAffected)
	return nil
}
