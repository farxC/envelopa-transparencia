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
	query := `INSERT INTO commitments (
		id,
		commitment_code,
		emission_date,
		document_code_type,
		document_type,
		favored_code,
		favored_name,
		management_unit_name,
		management_unit_code,
		management_code,
		management_name,
		extra_budgetary,
		process,
		original_commitment_value,
		converted_commitment_value,
		conversion_used_value,
		inserted_at,
		updated_at
	) VALUES (
		:id,	 	
		:commitment_code,
		:emission_date,
		:document_code_type,
		:document_type,
		:favored_code,
		:favored_name,
		:management_unit_name,
		:management_unit_code,
		:management_code,
		:management_name,
		:extra_budgetary,
		:process,
		:original_commitment_value,
		:converted_commitment_value,
		:conversion_used_value,
		:inserted_at,
		:updated_at
	)`

	result, err := cs.db.NamedExec(query, commitment)

	if err != nil {
		return err
	}
	rowsAffected, _ := result.RowsAffected()
	log.Printf("Insertion successful, %d rows affected.", rowsAffected)
	return nil
}

func (cs *CommitmentStore) InsertCommitmentItem(ctx context.Context, item *CommitmentItem) error {
	query := `INSERT INTO commitment_items (
		commitment_id,
		commitment_code,
		expense_nature_code,
		expense_category,
		expense_group,
		expense_element,
		description,
		quantity,
		sequential,
		unit_price,
		current_value,
		current_price,
		total_price,
		inserted_at,
		updated_at
	) VALUES (
		:commitment_id,
		:commitment_code,
		:expense_nature_code,
		:expense_category,
		:expense_group,
		:expense_element,
		:description,
		:quantity,
		:sequential,
		:unit_price,
		:current_value,
		:current_price,
		:total_price,
		:inserted_at,
		:updated_at
	)`

	result, err := cs.db.NamedExec(query, item)

	if err != nil {
		return err
	}
	rowsAffected, _ := result.RowsAffected()
	log.Printf("Insertion successful, %d rows affected.", rowsAffected)
	return nil
}
func (cs *CommitmentStore) InsertCommitmentItemHistory(ctx context.Context, history *CommitmentItemsHistory) error {
	query := `INSERT INTO commitment_items_history (
		commitment_id,
		commitment_code,
		operation_type,
		item_quantity,
		sequential,
		item_unit_price,
		item_total_price,
		operation_date,
		inserted_at,
		updated_at
	) VALUES (
		:commitment_id,
		:commitment_code,
		:operation_type,
		:item_quantity,
		:sequential,
		:item_unit_price,
		:item_total_price,
		:operation_date,
		:inserted_at,
		:updated_at
	)`

	result, err := cs.db.NamedExec(query, history)

	if err != nil {
		return err
	}
	rowsAffected, _ := result.RowsAffected()
	log.Printf("Insertion successful, %d rows affected.", rowsAffected)
	return nil
}
