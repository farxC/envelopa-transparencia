package store

import (
	"context"
	"log"

	"github.com/jmoiron/sqlx"
)

type LiquidationStore struct {
	db *sqlx.DB
}

func (ls *LiquidationStore) InsertLiquidation(ctx context.Context, liquidation *Liquidation) error {
	query := `INSERT INTO liquidations (
		liquidation_code,
		liquidation_code_resumed,
		liquidation_emission_date,
		document_code_type,
		document_type,
		management_unit_name,
		management_unit_code,
		management_code,
		management_name,
		favored_code,
		favored_name,
		observation,
		inserted_at,
		updated_at
	) VALUES (
		:liquidation_code,
		:liquidation_code_resumed,
		:liquidation_emission_date,
		:document_code_type,
		:document_type,
		:management_unit_name,
		:management_unit_code,
		:management_code,
		:management_name,
		:favored_code,
		:favored_name,
		:observation,
		:inserted_at,
		:updated_at
	)`

	result, err := ls.db.NamedExec(query, liquidation)

	if err != nil {
		return err
	}
	rowsAffected, _ := result.RowsAffected()
	log.Printf("Inserted %d rows into liquidations table", rowsAffected)
	return nil
}

func (ls *LiquidationStore) InsertLiquidationImpactedCommitment(ctx context.Context, lic *LiquidationImpactedCommitment) error {
	query := `INSERT INTO liquidation_impacted_commitments (
		commitment_code,
		liquidation_code,
		expense_nature_code,
		subitem,
		liquidated_value_brl,
		registered_payables_value_brl,
		canceled_payables_value_brl,
		outstanding_value_liquidated_brl,
		inserted_at,
		updated_at
	) VALUES (
		:commitment_code,
		:liquidation_code,
		:expense_nature_code,
		:subitem,
		:liquidated_value_brl,
		:registered_payables_value_brl,
		:canceled_payables_value_brl,
		:outstanding_value_liquidated_brl,
		:inserted_at,
		:updated_at
	)`

	result, err := ls.db.NamedExec(query, lic)

	if err != nil {
		return err
	}
	rowsAffected, _ := result.RowsAffected()
	log.Printf("Inserted %d rows into liquidation_impacted_commitments table", rowsAffected)
	return nil
}
