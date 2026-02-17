package main

import (
	"net/http"
	"strconv"

	"github.com/farxc/envelopa-transparencia/internal/response"
	"github.com/farxc/envelopa-transparencia/internal/store"
)

type (
	GetExpensesReportResponse     = response.APIResponse[store.BudgetExecutionReportByUnit]
	GetExpensesSummaryResponse    = response.APIResponse[store.SummaryByUnits]
	GetGlobalSummaryResponse      = response.APIResponse[store.GlobalSummary]
	GetTopFavoredResponse         = response.APIResponse[[]store.TopFavored]
	GetExpensesByCategoryResponse = response.APIResponse[[]store.ExpensesByCategory]
)

// @Summary		Get expenses summary
// @Description	Get a summary of expenses by applying various filters.
// @Tags			Expenses
// @Produce		json
// @Param			management_code			query		int							true	"Management code (required)"
// @Param			management_unit_codes	query		string						false	"Comma-separated list of management unit codes (optional)"
// @Param			start_date				query		string						false	"Start date for filtering (YYYY-MM-DD, optional)"
// @Param			end_date				query		string						false	"End date for filtering (YYYY-MM-DD, optional)"
// @Success		200						{object}	GetExpensesSummaryResponse	"Successfully retrieved expenses summary"
// @Failure		400						{object}	response.ErrorResponse		"Invalid request payload"
// @Failure		500						{object}	response.ErrorResponse		"Failed to filter expenses table"
// @Router			/expenses/summary [get]
func (app *application) handleGetExpensesSummary(w http.ResponseWriter, r *http.Request) {
	filter, err := parseExpensesFilter(r)

	if err != nil {
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}

	ctx := r.Context()
	data, err := app.store.Expenses.GetBudgetExecutionSummary(ctx, filter)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "failed to filter expenses table: "+err.Error())
		return
	}

	response := &GetExpensesSummaryResponse{
		Success: true,
		Data:    data,
		Message: "Successfully filtered expenses table by units",
	}

	if err := writeJSON(w, http.StatusOK, response); err != nil {
		writeJSONError(w, http.StatusInternalServerError, "failed to write response")
	}
}

// @Summary		Get global expenses summary
// @Description	Get a global summary of expenses by applying various filters.
// @Tags			Expenses
// @Produce		json
// @Param			management_code	query		int							true	"Management code (required)"
// @Param			start_date		query		string						false	"Start date for filtering (YYYY-MM-DD, optional)"
// @Param			end_date		query		string						false	"End date for filtering (YYYY-MM-DD, optional)"
// @Success		200				{object}	GetGlobalSummaryResponse	"Successfully retrieved global expenses summary"
// @Failure		400				{object}	response.ErrorResponse		"Invalid request payload"
// @Failure		500				{object}	response.ErrorResponse		"Failed to get global expenses summary"
// @Router			/expenses/summary/by-management [get]
func (app *application) handleGetExpensesSummaryByManagement(w http.ResponseWriter, r *http.Request) {
	filter, err := parseExpensesFilter(r)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}

	ctx := r.Context()
	data, err := app.store.Expenses.GetBudgetExecutionSummaryByManagement(ctx, filter)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "failed to get global expenses summary: "+err.Error())
		return
	}

	response := &GetGlobalSummaryResponse{
		Success: true,
		Data:    data,
		Message: "Successfully retrieved global budget execution summary",
	}

	if err := writeJSON(w, http.StatusOK, response); err != nil {
		writeJSONError(w, http.StatusInternalServerError, "failed to write response")
	}
}

// @Summary		Get budget execution report
// @Description	Get a budget execution report by applying various filters.
// @Tags			Expenses
// @Produce		json
// @Param			management_code			query		int							true	"Management code (required)"
// @Param			management_unit_codes	query		string						false	"Comma-separated list of management unit codes (optional)"
// @Param			start_date				query		string						false	"Start date for filtering (YYYY-MM-DD, optional)"
// @Param			end_date				query		string						false	"End date for filtering (YYYY-MM-DD, optional)"
// @Success		200						{object}	GetExpensesReportResponse	"Successfully retrieved budget execution report"
// @Failure		400						{object}	response.ErrorResponse		"Invalid request payload"
// @Failure		500						{object}	response.ErrorResponse		"Failed to get budget execution report"
// @Router			/expenses/budget-execution/report [get]
func (app *application) handleGetBudgetExecutionReport(w http.ResponseWriter, r *http.Request) {
	filter, err := parseExpensesFilter(r)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}

	ctx := r.Context()
	data, err := app.store.Expenses.GetBudgetExecutionReport(ctx, filter)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "failed to get budget execution report: "+err.Error())
		return
	}

	response := &GetExpensesReportResponse{
		Success: true,
		Data:    data,
		Message: "Successfully retrieved budget execution report",
	}

	if err := writeJSON(w, http.StatusOK, response); err != nil {
		writeJSONError(w, http.StatusInternalServerError, "failed to write response")
	}
}

// @Summary		Get top favored entities
// @Description	Get a list of top favored entities by applying various filters.
// @Tags			Expenses
// @Produce		json
// @Param			management_code			query		int						true	"Management code (required)"
// @Param			management_unit_codes	query		string					false	"Comma-separated list of management unit codes (optional)"
// @Param			start_date				query		string					false	"Start date for filtering (YYYY-MM-DD, optional)"
// @Param			end_date				query		string					false	"End date for filtering (YYYY-MM-DD, optional)"
// @Param			limit					query		int						false	"Limit the number of results"	default(10)
// @Success		200						{object}	GetTopFavoredResponse	"Successfully retrieved top favored entities"
// @Failure		400						{object}	response.ErrorResponse	"Invalid request payload"
// @Failure		500						{object}	response.ErrorResponse	"Failed to get top favored"
// @Router			/expenses/top-favored [get]
func (app *application) handleGetTopFavored(w http.ResponseWriter, r *http.Request) {
	filter, err := parseExpensesFilter(r)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}

	limitParam := r.URL.Query().Get("limit")
	limit := 10
	if limitParam != "" {
		if l, err := strconv.Atoi(limitParam); err == nil {
			limit = l
		}
	}

	ctx := r.Context()
	data, err := app.store.Expenses.GetTopFavored(ctx, filter, limit)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "failed to get top favored: "+err.Error())
		return
	}

	response := &GetTopFavoredResponse{
		Success: true,
		Data:    data,
		Message: "Successfully retrieved top favored entities",
	}

	if err := writeJSON(w, http.StatusOK, response); err != nil {
		writeJSONError(w, http.StatusInternalServerError, "failed to write response")
	}
}
