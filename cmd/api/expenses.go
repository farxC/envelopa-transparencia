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

type (
	GetExpensesReportResponse     = response.APIResponse[store.BudgetExecutionReportByUnit]
	GetExpensesSummaryResponse    = response.APIResponse[store.SummaryByUnits]
	GetGlobalSummaryResponse      = response.APIResponse[store.GlobalSummary]
	GetTopFavoredResponse         = response.APIResponse[[]store.TopFavored]
	GetExpensesByCategoryResponse = response.APIResponse[[]store.ExpensesByCategory]
)

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
	var filter store.ExpensesFilter

	// Required: management_code
	managementCode := r.URL.Query().Get("management_code")
	if managementCode == "" {
		return filter, fmt.Errorf("management_code is required")
	}
	managementCodeInt, err := strconv.Atoi(managementCode)
	if err != nil {
		return filter, fmt.Errorf("invalid management_code: %w", err)
	}
	filter.ManagementCode = managementCodeInt

	// Optional: management_unit_codes
	codesParam := r.URL.Query().Get("management_unit_codes")
	if codesParam != "" {
		if !isValidCodes(codesParam) {
			return filter, fmt.Errorf("invalid codes parameter")
		}
		filter.ManagementUnitCodes = make([]int, 0)
		for _, code := range strings.Split(codesParam, ",") {
			codeInt, err := strconv.Atoi(code)
			if err != nil {
				return filter, fmt.Errorf("invalid management_unit_code: %w", err)
			}
			filter.ManagementUnitCodes = append(filter.ManagementUnitCodes, codeInt)
		}
	}

	// Optional: date range
	startParam := r.URL.Query().Get("start_date")
	endParam := r.URL.Query().Get("end_date")

	if startParam != "" {
		startDate, err := time.Parse("2006-01-02", startParam)
		if err != nil {
			return filter, fmt.Errorf("invalid start_date format (expected YYYY-MM-DD)")
		}
		filter.StartDate = startDate
	}

	if endParam != "" {
		endDate, err := time.Parse("2006-01-02", endParam)
		if err != nil {
			return filter, fmt.Errorf("invalid end_date format (expected YYYY-MM-DD)")
		}
		filter.EndDate = endDate
	}

	return filter, nil
}

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
