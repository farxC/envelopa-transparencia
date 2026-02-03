package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"
)

type ExpensesStore struct {
	db *sqlx.DB
}

type ScopeType string

const (
	ByManagementUnit ScopeType = "management_unit"
	ByManagement     ScopeType = "management"
)

type ExpensesFilter struct {
	StartDate time.Time `json:"start_date"`
	EndDate   time.Time `json:"end_date"`
	ScopeType ScopeType `json:"scope_type"`
	Code      int       `json:"code"`
}

type ExpensesTableResult struct {
	CommittedAmount       float64 `db:"valor_empenhado" json:"valor_empenhado"`
	LiquidatedAmount      float64 `db:"valor_liquidado" json:"valor_liquidado"`
	PaidAmount            float64 `db:"valor_pago" json:"valor_pago"`
	BalanceToLiquidate    float64 `db:"saldo_a_liquidar" json:"saldo_a_liquidar"`
	BalanceToPayProcessed float64 `db:"saldo_a_pagar_processado" json:"saldo_a_pagar_processado"`
	ExecutionPercentage   float64 `db:"percentual_execucao" json:"percentual_execucao"`
}

func (es *ExpensesStore) SearchConsolidatedExpensesByExpensesNature(ctx context.Context, e ExpensesFilter) (map[string]float64, error) {
	var whereClause string

	if e.ScopeType == ByManagementUnit {
		whereClause = "c.management_unit_code = $3"
	} else {
		whereClause = "c.management_code = $3"
	}

	query := `
		SELECT 
			CONCAT(
				c.expense_category_code, '.', 
				c.expense_group_code, '.', 
				LPAD(c.application_modality_code::text, 2, '0'), '.', 
				LPAD(c.expense_element_code::text, 2, '0')
			) AS natureza_despesa_completa,
			c.expense_element AS descricao_elemento,
			SUM(COALESCE(ci.current_value, 0)) AS valor_total_empenhado
		FROM
			commitments c 
		LEFT JOIN 
			commitment_items ci ON c.id = ci.commitment_id 
		WHERE 
			c.emission_date BETWEEN $1 AND $2
		AND
			` + whereClause + `
		GROUP BY
			c.expense_category_code,
			c.expense_group_code,
			c.application_modality_code,
			c.expense_element_code,
			c.expense_element
		ORDER BY 
			valor_total_empenhado DESC;
	`
	rows, err := es.db.QueryxContext(ctx, query, e.StartDate, e.EndDate, e.Code)
	if err != nil {
		return nil, fmt.Errorf("failed to query expenses by nature: %w", err)
	}
	defer rows.Close()

	result := make(map[string]float64)

	for rows.Next() {
		var naturezaDespesa string
		var valorPorNatureza float64
		var descricaoElemento string

		err := rows.Scan(&naturezaDespesa, &descricaoElemento, &valorPorNatureza)
		if err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}

		result[naturezaDespesa] = valorPorNatureza
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("rows iteration error: %w", err)
	}

	return result, nil
}

func (es *ExpensesStore) FilterExpensesTable(ctx context.Context, e ExpensesFilter) (ExpensesTableResult, error) {
	w := make(map[string]string)

	if e.ScopeType == ByManagementUnit {
		w["liquidation"] = "l.management_unit_code = $3"
		w["payment"] = "p.management_unit_code = $3"
		w["commitment"] = "c.management_unit_code = $3"
	} else {
		w["liquidation"] = "l.management_code = $3"
		w["payment"] = "p.management_code = $3"
		w["commitment"] = "c.management_code = $3"
	}

	query := `
	WITH TotalEmpenhado AS (
		SELECT 
			COALESCE(SUM(ci.current_value), 0) AS valor_empenhado
		FROM 
			commitments c 
		LEFT JOIN 
			commitment_items ci ON c.id = ci.commitment_id 
		WHERE 
			c.expense_category_code = 3 -- Custeio
		AND
			c.emission_date BETWEEN $1 AND $2
		AND 
			` + w["commitment"] + `
	),

	TotalLiquidado AS (
		SELECT 
			COALESCE(SUM(lic.liquidated_value_brl), 0) AS valor_liquidado
		FROM 
			liquidations l
		LEFT JOIN 
			liquidation_impacted_commitments lic ON l.liquidation_code = lic.liquidation_code
		WHERE 
			lic.expense_nature_code LIKE '33%' -- Formato sem ponto
		AND
			l.liquidation_emission_date BETWEEN $1 AND $2
		AND 
			` + w["liquidation"] + `
	),

	TotalPago AS (
		SELECT 
			COALESCE(SUM(pic.paid_value_brl), 0) AS valor_pago
		FROM 
			payments p
		LEFT JOIN 
			payment_impacted_commitments pic ON p.payment_code = pic.payment_code 
		WHERE 
			pic.expense_nature_code_complete LIKE '33%' -- Formato sem ponto
		AND
			p.payment_emission_date BETWEEN $1 AND $2
		AND 
			` + w["payment"] + `
	)

	-- 4. Consolidação
	SELECT 
		e.valor_empenhado,
		l.valor_liquidado,
		p.valor_pago,
		(e.valor_empenhado - l.valor_liquidado) AS saldo_a_liquidar,
		(l.valor_liquidado - p.valor_pago) AS saldo_a_pagar_processado,
		CASE 
			WHEN e.valor_empenhado > 0 THEN ROUND((p.valor_pago / e.valor_empenhado) * 100, 2)
			ELSE 0 
		END AS percentual_execucao
	FROM 
		TotalEmpenhado e
		CROSS JOIN TotalLiquidado l
		CROSS JOIN TotalPago p;
	`

	var result ExpensesTableResult

	err := es.db.GetContext(ctx, &result, query, e.StartDate, e.EndDate, e.Code)
	if err != nil {
		if err == sql.ErrNoRows {
			return ExpensesTableResult{
				CommittedAmount:       0,
				LiquidatedAmount:      0,
				PaidAmount:            0,
				BalanceToLiquidate:    0,
				BalanceToPayProcessed: 0,
				ExecutionPercentage:   0,
			}, nil
		}
		return ExpensesTableResult{}, fmt.Errorf("failed to query consolidated expenses: %w", err)
	}

	return ExpensesTableResult{
		CommittedAmount:       result.CommittedAmount,
		LiquidatedAmount:      result.LiquidatedAmount,
		PaidAmount:            result.PaidAmount,
		BalanceToLiquidate:    result.BalanceToLiquidate,
		BalanceToPayProcessed: result.BalanceToPayProcessed,
		ExecutionPercentage:   result.ExecutionPercentage,
	}, nil
}
