package main

import (
	"net/http"

	"github.com/farxc/envelopa-transparencia/internal/domain/response"
	"github.com/farxc/envelopa-transparencia/internal/domain/service"
)

type GetBudgetExecutionResponse = response.APIResponse[[]service.BudgetExecutionRow]

// @Summary		Get budget execution rows
// @Description	Get budget execution rows from expenses_execution table by applying various filters.
// @Tags			BudgetExecution
// @Produce		json
// @Param			management_code			query		int							true	"Management code (required)"
// @Param			management_unit_codes	query		string						false	"Comma-separated list of management unit codes (optional)"
// @Param			start_date				query		string						false	"Start date for filtering (YYYY-MM-DD, optional)"
// @Param			end_date				query		string						false	"End date for filtering (YYYY-MM-DD, optional)"
// @Success		200						{object}	GetBudgetExecutionResponse	"Successfully retrieved budget execution rows"
// @Failure		400						{object}	response.ErrorResponse		"Invalid request payload"
// @Failure		500						{object}	response.ErrorResponse		"Failed to get budget execution"
// @Router			/budget-execution/ [get]
func (app *application) handleGetBudgetExecution(w http.ResponseWriter, r *http.Request) {
	filter, err := parseExpensesFilter(r)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}

	ctx := r.Context()
	data, err := app.store.ExpensesExecution.GetBudgetExecution(ctx, filter)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "failed to get budget execution: "+err.Error())
		return
	}

	resp := &GetBudgetExecutionResponse{
		Success: true,
		Data:    data,
		Message: "Successfully retrieved budget execution rows",
	}

	if err := writeJSON(w, http.StatusOK, resp); err != nil {
		writeJSONError(w, http.StatusInternalServerError, "failed to write response")
	}
}
