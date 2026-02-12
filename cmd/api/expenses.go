package main

import (
	"fmt"
	"net/http"
	"time"

	"github.com/farxc/envelopa-transparencia/internal/response"
	"github.com/farxc/envelopa-transparencia/internal/store"
)

type GetExpensesReportResponse = response.APIResponse[store.BudgetExecutionReportByUnit]
type GetExpensesSummaryResponse = response.APIResponse[store.SummaryByUnits]

func FormatCodesToString(codes []int) string {
	codesToString := ""
	for i, code := range codes {
		if i > 0 {
			codesToString += ","
		}
		codesToString += fmt.Sprintf("%d", code)
	}
	return codesToString
}

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

func (app *application) handleGetExpensesSummary(w http.ResponseWriter, r *http.Request) {
	startParam := r.URL.Query().Get("start_date")
	endParam := r.URL.Query().Get("end_date")
	codeParam := r.URL.Query().Get("codes")

	var filter store.ExpensesFilter

	filter.StartDate, _ = time.Parse("2006-01-02", parseDateOrDefault(startParam, "2000-01-01"))
	filter.EndDate, _ = time.Parse("2006-01-02", parseDateOrDefault(endParam, "2100-12-31"))
	if !isValidCodes(codeParam) {
		writeJSONError(w, http.StatusBadRequest, "invalid codes parameter")
		return
	}
	filter.Codes = codeParam
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

func (app *application) handleGetBudgetExecutionReport(w http.ResponseWriter, r *http.Request) {
	startParam := r.URL.Query().Get("start_date")
	endParam := r.URL.Query().Get("end_date")
	codeParam := r.URL.Query().Get("codes")

	var filter store.ExpensesFilter

	filter.StartDate, _ = time.Parse("2006-01-02", parseDateOrDefault(startParam, "2000-01-01"))
	filter.EndDate, _ = time.Parse("2006-01-02", parseDateOrDefault(endParam, "2100-12-31"))
	if !isValidCodes(codeParam) {
		writeJSONError(w, http.StatusBadRequest, "invalid codes parameter")
		return
	}
	filter.Codes = codeParam
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
