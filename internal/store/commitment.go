package store

import (
	"context"

	"github.com/jmoiron/sqlx"
)

type CommitmentStore struct {
	db *sqlx.DB
}

func (cs *CommitmentStore) InsertCommitment(ctx context.Context, commitment *Commitment) error {
	query := `INSERT INTO commitments (
		id,
		commitment_code,
		resumed_commitment_code,
		emission_date,
		type,
		process,
		document_code_type,
		document_type,
		management_unit_name,
		management_unit_code,
		management_code,
		management_name,
		favored_name,
		favored_code,
		expense_category_code,
		expense_category,
		expense_group_code,
		expense_group,
		application_modality_code,
		application_modality,
		expense_element_code,
		expense_element,
		budget_plan,
		budget_plan_code,
		observation,
		commitment_original_value,
		commitment_value_converted_to_brl,
		conversion_value_used,
		inserted_at,
		updated_at
	) VALUES (
		:id,
		:commitment_code,
		:resumed_commitment_code,
		:emission_date,
		:type,
		:process,
		:document_code_type,
		:document_type,
		:management_unit_name,
		:management_unit_code,
		:management_code,
		:management_name,
		:favored_name,
		:favored_code,
		:expense_category_code,
		:expense_category,
		:expense_group_code,
		:expense_group,
		:application_modality_code,
		:application_modality,
		:expense_element_code,
		:expense_element,
		:budget_plan,
		:budget_plan_code,
		:observation,
		:commitment_original_value,
		:commitment_value_converted_to_brl,
		:conversion_value_used,
		:inserted_at,
		:updated_at
	)`

	_, err := cs.db.NamedExec(query, commitment)

	if err != nil {
		return err
	}

	return nil
}

func (cs *CommitmentStore) InsertCommitmentItem(ctx context.Context, item *CommitmentItem) error {
	query := `INSERT INTO commitment_items (
		commitment_id,
		commitment_code,
		expense_category_code,
		expense_category,
		expense_group_code,
		expense_group,
		application_modality_code,
		application_modality,
		expense_element_code,
		expense_element,
		sub_expense_element,
		sub_expense_element_code,
		observation,
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
		:expense_category_code,
		:expense_category,
		:expense_group_code,
		:expense_group,
		:application_modality_code,
		:application_modality,
		:expense_element_code,
		:expense_element,
		:sub_expense_element,
		:sub_expense_element_code,
		:observation,
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

	_, err := cs.db.NamedExec(query, item)

	if err != nil {
		return err
	}
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

	_, err := cs.db.NamedExec(query, history)

	if err != nil {
		return err
	}
	return nil
}
