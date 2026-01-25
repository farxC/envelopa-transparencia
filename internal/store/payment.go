package store

import (
	"context"
	"log"

	"github.com/jmoiron/sqlx"
)

type PaymentStore struct {
	db *sqlx.DB
}

func (ps *PaymentStore) InsertPayment(ctx context.Context, payment *Payment) error {
	query := `INSERT INTO payments (
		payment_code,
		payment_code_resumed,
		payment_emission_date,
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
		original_payment_value,
		converted_payment_value,
		conversion_used_value,
		inserted_at,
		updated_at
	) VALUES (
		:payment_code,
		:payment_code_resumed,
		:payment_emission_date,
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
		:original_payment_value,
		:converted_payment_value,
		:conversion_used_value,
		:inserted_at,
		:updated_at
	)`

	result, err := ps.db.NamedExec(query, payment)

	if err != nil {
		return err
	}
	rowsAffected, _ := result.RowsAffected()
	log.Printf("Inserted %d rows into payments table", rowsAffected)
	return nil
}

func (ps *PaymentStore) InsertPaymentImpactedCommitment(ctx context.Context, pic *PaymentImpactedCommitment) error {
	query := `INSERT INTO payment_impacted_commitments (
		commitment_code,
		payment_code,
		expense_nature_code_complete,
		subitem,
		paid_value_brl,
		registered_payables_value_brl,
		canceled_payables_value_brl,
		outstanding_value_paid_brl,
		inserted_at,
		updated_at
	) VALUES (
		:commitment_code,
		:payment_code,
		:expense_nature_code_complete,
		:subitem,
		:paid_value_brl,
		:registered_payables_value_brl,
		:canceled_payables_value_brl,
		:outstanding_value_paid_brl,
		:inserted_at,
		:updated_at
	)`

	result, err := ps.db.NamedExec(query, pic)

	if err != nil {
		return err
	}
	rowsAffected, _ := result.RowsAffected()
	log.Printf("Inserted %d rows into payment_impacted_commitments table", rowsAffected)
	return nil
}
