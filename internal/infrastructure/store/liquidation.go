package store

import (
	"context"

	"github.com/farxc/envelopa-transparencia/internal/domain/model"
)

type LiquidationStore struct {
	db GenericQueryer
}

func (ls *LiquidationStore) InsertLiquidation(ctx context.Context, liquidation *model.Liquidation) error {
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
		:inserted_at,
		:updated_at
	)
		ON CONFLICT (liquidation_code) DO UPDATE SET
		liquidation_code_resumed = EXCLUDED.liquidation_code_resumed,
		liquidation_emission_date = EXCLUDED.liquidation_emission_date,
		document_code_type = EXCLUDED.document_code_type,
		document_type = EXCLUDED.document_type,
		management_unit_name = EXCLUDED.management_unit_name,
		management_unit_code = EXCLUDED.management_unit_code,
		management_code = EXCLUDED.management_code,
		management_name = EXCLUDED.management_name,
		favored_code = EXCLUDED.favored_code,
		favored_name = EXCLUDED.favored_name,
		expense_category_code = EXCLUDED.expense_category_code,
		expense_category = EXCLUDED.expense_category,
		expense_group_code = EXCLUDED.expense_group_code,
		expense_group = EXCLUDED.expense_group,
		application_modality_code = EXCLUDED.application_modality_code,
		application_modality = EXCLUDED.application_modality,
		expense_element_code = EXCLUDED.expense_element_code,
		expense_element = EXCLUDED.expense_element,
		budget_plan = EXCLUDED.budget_plan,
		budget_plan_code = EXCLUDED.budget_plan_code,
		observation = EXCLUDED.observation,
		updated_at = EXCLUDED.updated_at
	`

	_, err := ls.db.NamedExec(query, liquidation)
	if err != nil {
		return err
	}
	return nil
}

func (ls *LiquidationStore) InsertLiquidationImpactedCommitment(ctx context.Context, lic *model.LiquidationImpactedCommitment) error {
	query := `INSERT INTO liquidation_impacted_commitments (
		commitment_code,
		liquidation_code,
		expense_nature_code_complete,
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
		:expense_nature_code_complete,
		:subitem,
		:liquidated_value_brl,
		:registered_payables_value_brl,
		:canceled_payables_value_brl,
		:outstanding_value_liquidated_brl,
		:inserted_at,
		:updated_at
	)
		ON CONFLICT (liquidation_code, commitment_code, expense_nature_code_complete, subitem) DO UPDATE SET
		liquidated_value_brl = EXCLUDED.liquidated_value_brl,
		registered_payables_value_brl = EXCLUDED.registered_payables_value_brl,
		canceled_payables_value_brl = EXCLUDED.canceled_payables_value_brl,
		outstanding_value_liquidated_brl = EXCLUDED.outstanding_value_liquidated_brl,
		updated_at = EXCLUDED.updated_at
	`

	_, err := ls.db.NamedExec(query, lic)
	if err != nil {
		return err
	}
	return nil
}

func (ls *LiquidationStore) DeleteImpactedCommitments(ctx context.Context, liquidationCode string) error {
	_, err := ls.db.ExecContext(ctx, `DELETE FROM liquidation_impacted_commitments WHERE liquidation_code = $1`, liquidationCode)
	return err
}
