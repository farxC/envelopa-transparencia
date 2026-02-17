package store

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/lib/pq"
)

type CommitmentStore struct {
	db GenericQueryer
}

type GetCommitmentInformationFilter struct {
	CommitmentCodes     []string
	ManagementUnitCodes []string
	ManagementCode      string
	StartDate           time.Time
	EndDate             time.Time
}

type CommitmentItemInformation struct {
	ItemDescription   string  `db:"item_description" json:"item_description"`
	CurrentValue      float64 `db:"current_value" json:"current_value"`
	SubExpenseElement string  `db:"sub_expense_element" json:"sub_expense_element"`
	Description       string  `db:"description" json:"description"`
	Quantity          float64 `db:"quantity" json:"quantity"`
	Sequential        int     `db:"sequential" json:"sequential"`
}

type CommitmentInformation struct {
	ManagementUnitCode     string                      `db:"management_unit_code" json:"management_unit_code"`
	CommitmentCode         string                      `db:"commitment_code" json:"commitment_code"`
	CommitmentTotalValue   float64                     `db:"commitment_total_value" json:"commitment_total_value"`
	CommitmentEmissionDate time.Time                   `db:"commitment_emission_date" json:"commitment_emission_date"`
	CommitmentProcess      string                      `db:"commitment_process" json:"commitment_process"`
	CommitmentType         string                      `db:"commitment_type" json:"commitment_type"`
	CommitmentFavored      string                      `db:"commitment_favored" json:"commitment_favored"`
	CommitmentFavoredCode  string                      `db:"commitment_favored_code" json:"commitment_favored_code"`
	CommitmentItemsRaw     json.RawMessage             `db:"commitment_items" json:"-"`
	CommitmentItems        []CommitmentItemInformation `json:"commitment_items"`
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
	)
		ON CONFLICT (commitment_code) DO UPDATE SET
		resumed_commitment_code = EXCLUDED.resumed_commitment_code,
		emission_date = EXCLUDED.emission_date,
		type = EXCLUDED.type,
		process = EXCLUDED.process,
		document_code_type = EXCLUDED.document_code_type,
		document_type = EXCLUDED.document_type,
		management_unit_name = EXCLUDED.management_unit_name,
		management_unit_code = EXCLUDED.management_unit_code,
		management_code = EXCLUDED.management_code,
		management_name = EXCLUDED.management_name,
		favored_name = EXCLUDED.favored_name,
		favored_code = EXCLUDED.favored_code,
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
		commitment_original_value = EXCLUDED.commitment_original_value,
		commitment_value_converted_to_brl = EXCLUDED.commitment_value_converted_to_brl,
		conversion_value_used = EXCLUDED.conversion_value_used,
		inserted_at = EXCLUDED.inserted_at,
		updated_at = EXCLUDED.updated_at
	`

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
		:description,
		:quantity,
		:sequential,
		:unit_price,
		:current_value,
		:current_price,
		:total_price,
		:inserted_at,
		:updated_at
	)
		ON CONFLICT (commitment_code, sequential) DO UPDATE SET
		commitment_id = EXCLUDED.commitment_id,
		commitment_code = EXCLUDED.commitment_code,
	  	expense_category_code = EXCLUDED.expense_category_code,
		expense_category = EXCLUDED.expense_category,
		expense_group_code = EXCLUDED.expense_group_code,
		expense_group = EXCLUDED.expense_group,
		application_modality_code = EXCLUDED.application_modality_code,
		application_modality = EXCLUDED.application_modality,
		expense_element_code = EXCLUDED.expense_element_code,
		expense_element = EXCLUDED.expense_element,
		sub_expense_element = EXCLUDED.sub_expense_element,
		sub_expense_element_code = EXCLUDED.sub_expense_element_code,
		description = EXCLUDED.description,
		quantity = EXCLUDED.quantity,
		sequential = EXCLUDED.sequential,
		unit_price = EXCLUDED.unit_price,
		current_value = EXCLUDED.current_value,
		current_price = EXCLUDED.current_price,
		total_price = EXCLUDED.total_price,
		inserted_at = EXCLUDED.inserted_at,
		updated_at = EXCLUDED.updated_at
		
	`

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
	)
		ON CONFLICT (commitment_code, sequential, operation_date, operation_type) DO UPDATE SET
		commitment_id = EXCLUDED.commitment_id,
		commitment_code = EXCLUDED.commitment_code,
		operation_type = EXCLUDED.operation_type,
		item_quantity = EXCLUDED.item_quantity,
		sequential = EXCLUDED.sequential,
		item_unit_price = EXCLUDED.item_unit_price,
		item_total_price = EXCLUDED.item_total_price,
		operation_date = EXCLUDED.operation_date,
		inserted_at = EXCLUDED.inserted_at,
		updated_at = EXCLUDED.updated_at
	`

	_, err := cs.db.NamedExec(query, history)
	if err != nil {
		return err
	}
	return nil
}

func (cs *CommitmentStore) GetCommitmentInformation(ctx context.Context, filter GetCommitmentInformationFilter) ([]CommitmentInformation, error) {
	whereClause := "WHERE c.management_code = $1"
	args := []interface{}{filter.ManagementCode}
	argIndex := 2
	// Optional additional filters
	if len(filter.CommitmentCodes) > 0 {
		whereClause += fmt.Sprintf(" AND c.commitment_code = ANY($%d)", argIndex)
		args = append(args, pq.Array(filter.CommitmentCodes))
		argIndex++
	}

	if len(filter.ManagementUnitCodes) > 0 {
		whereClause += fmt.Sprintf(" AND c.management_unit_code = ANY($%d)", argIndex)
		args = append(args, pq.Array(filter.ManagementUnitCodes))
		argIndex++
	}

	if !filter.StartDate.IsZero() && !filter.EndDate.IsZero() {
		whereClause += fmt.Sprintf(" AND c.emission_date BETWEEN $%d AND $%d", argIndex, argIndex+1)
		args = append(args, filter.StartDate, filter.EndDate)
	}

	q := fmt.Sprintf(`
	SELECT 
		c.management_unit_code as management_unit_code,
		c.commitment_code,
		SUM(ci.current_value) as commitment_total_value,
		c.emission_date as commitment_emission_date,
		c.process as commitment_process,
		c.type as commitment_type,
		c.favored_name as commitment_favored,
		c.favored_code as commitment_favored_code,
		JSON_AGG(JSON_BUILD_OBJECT(
			'item_description', ci.description,
			'current_value', ci.current_value,
			'sub_expense_element', ci.sub_expense_element,
			'description', ci.description,
			'quantity', ci.quantity,
			'sequential', ci.sequential
		)) AS commitment_items
	FROM 
		commitments c
	JOIN 
		commitment_items ci ON c.id = ci.commitment_id
	%s
	GROUP BY 
		c.management_unit_code, c.commitment_code, c.emission_date, c.process, c.type, c.favored_name, c.favored_code;
	`, whereClause)

	var c []CommitmentInformation
	err := cs.db.SelectContext(ctx, &c, q, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to get commitment information: %w", err)
	}

	for i := range c {
		if err := json.Unmarshal(c[i].CommitmentItemsRaw, &c[i].CommitmentItems); err != nil {
			return nil, fmt.Errorf("failed to unmarshal commitment items: %w", err)
		}
	}

	return c, nil
}
