package main

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/farxc/envelopa-transparencia/internal/response"
	"github.com/farxc/envelopa-transparencia/internal/store"
)

type GetExpensesReportResponse = response.APIResponse[store.BudgetExecutionReportByUnit]
type GetExpensesSummaryResponse = response.APIResponse[store.SummaryByUnits]
type GetGlobalSummaryResponse = response.APIResponse[store.GlobalSummary]
type GetTopFavoredResponse = response.APIResponse[[]store.TopFavored]
type GetExpensesByCategoryResponse = response.APIResponse[[]store.ExpensesByCategory]

func isValidCodes(codeParam string) bool {
	if codeParam == "" {
		return false
	}
	// Check if all characters are digits, separated by commas
	for _, r := range codeParam {
		if (r < '0' || r > '9') && r != ',' {
			return false
		}
	}

	return true
}

func parseExpensesFilter(r *http.Request) (store.ExpensesFilter, error) {
	startParam := r.URL.Query().Get("start_date")
	endParam := r.URL.Query().Get("end_date")
	codeParam := r.URL.Query().Get("codes")

	var filter store.ExpensesFilter

	startDate, err := time.Parse("2006-01-02", parseDateOrDefault(startParam, "2000-01-01"))
	if err != nil {
		return filter, fmt.Errorf("invalid start_date")
	}
	filter.StartDate = startDate

	endDate, err := time.Parse("2006-01-02", parseDateOrDefault(endParam, "2100-12-31"))
	if err != nil {
		return filter, fmt.Errorf("invalid end_date")
	}
	filter.EndDate = endDate

	if !isValidCodes(codeParam) {
		return filter, fmt.Errorf("invalid codes parameter")
	}
	filter.Codes = strings.Split(codeParam, ",")

	return filter, nil
}

// @Summary		Get expenses summary
// @Description	Get a summary of expenses by applying various filters.
// @Tags			Expenses
// @Produce		json
// @Param			start_date	query		string						false	"Start date for filtering (YYYY-MM-DD)"
// @Param			end_date	query		string						false	"End date for filtering (YYYY-MM-DD)"
// @Param			codes		query		string						false	"Comma-separated list of codes for filtering"
// @Success		200			{object}	GetExpensesSummaryResponse	"Successfully retrieved expenses summary"
// @Failure		400			{object}	response.ErrorResponse		"Invalid request payload"
// @Failure		500			{object}	response.ErrorResponse		"Failed to filter expenses table"
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
// @Param			start_date	query		string						false	"Start date for filtering (YYYY-MM-DD)"
// @Param			end_date	query		string						false	"End date for filtering (YYYY-MM-DD)"
// @Param			codes		query		string						false	"Comma-separated list of codes for filtering"
// @Success		200			{object}	GetGlobalSummaryResponse	"Successfully retrieved global expenses summary"
// @Failure		400			{object}	response.ErrorResponse		"Invalid request payload"
// @Failure		500			{object}	response.ErrorResponse		"Failed to get global expenses summary"
// @Router			/expenses/global-summary [get]
func (app *application) handleGetGlobalExpensesSummary(w http.ResponseWriter, r *http.Request) {
	filter, err := parseExpensesFilter(r)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}

	ctx := r.Context()
	data, err := app.store.Expenses.GetGlobalBudgetExecutionSummary(ctx, filter)
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
// @Param			start_date	query		string						false	"Start date for filtering (YYYY-MM-DD)"
// @Param			end_date	query		string						false	"End date for filtering (YYYY-MM-DD)"
// @Param			codes		query		string						false	"Comma-separated list of codes for filtering"
// @Success		200			{object}	GetExpensesReportResponse	"Successfully retrieved budget execution report"
// @Failure		400			{object}	response.ErrorResponse		"Invalid request payload"
// @Failure		500			{object}	response.ErrorResponse		"Failed to get budget execution report"
// @Router			/expenses/report [get]
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
// @Param			start_date	query		string					false	"Start date for filtering (YYYY-MM-DD)"
// @Param			end_date	query		string					false	"End date for filtering (YYYY-MM-DD)"
// @Param			codes		query		string					false	"Comma-separated list of codes for filtering"
// @Param			limit		query		int						false	"Limit the number of results"	default(10)
// @Success		200			{object}	GetTopFavoredResponse	"Successfully retrieved top favored entities"
// @Failure		400			{object}	response.ErrorResponse	"Invalid request payload"
// @Failure		500			{object}	response.ErrorResponse	"Failed to get top favored"
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

// @Summary		Get expenses by category
// @Description	Get a list of expenses by category by applying various filters.
// @Tags			Expenses
// @Produce		json
// @Param			start_date	query		string							false	"Start date for filtering (YYYY-MM-DD)"
// @Param			end_date	query		string							false	"End date for filtering (YYYY-MM-DD)"
// @Param			codes		query		string							false	"Comma-separated list of codes for filtering"
// @Success		200			{object}	GetExpensesByCategoryResponse	"Successfully retrieved expenses by category"
// @Failure		400			{object}	response.ErrorResponse			"Invalid request payload"
// @Failure		500			{object}	response.ErrorResponse			"Failed to get expenses by category"
// @Router			/expenses/by-category [get]
func (app *application) handleGetExpensesByCategory(w http.ResponseWriter, r *http.Request) {
	filter, err := parseExpensesFilter(r)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}

	ctx := r.Context()
	data, err := app.store.Expenses.GetExpensesByCategory(ctx, filter)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "failed to get expenses by category: "+err.Error())
		return
	}

	response := &GetExpensesByCategoryResponse{
		Success: true,
		Data:    data,
		Message: "Successfully retrieved expenses by category",
	}

	if err := writeJSON(w, http.StatusOK, response); err != nil {
		writeJSONError(w, http.StatusInternalServerError, "failed to write response")
	}
}
