package main

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/farxc/envelopa-transparencia/internal/domain/response"
	"github.com/farxc/envelopa-transparencia/internal/domain/service"
)

type GetBudgetResponse = response.APIResponse[[]service.BudgetRow]
type GetBudgetSummaryResponse = response.APIResponse[[]service.BudgetSummaryRow]
type GetBudgetGlobalSummaryResponse = response.APIResponse[[]service.BudgetGlobalSummaryRow]

// @Summary		Get annual budget allocation rows
// @Description	Get raw expense budget rows filtered by one or more subordinate agency codes and optionally by exercise year.
// @Tags			Budget
// @Produce		json
// @Param			subordinate_agency_codes	query		string					true	"Comma-separated subordinate agency codes (e.g. 26421 or 26421,26415)"
// @Param			exercise					query		int						false	"Exercise year for filtering (e.g. 2025, optional)"
// @Success		200							{object}	GetBudgetResponse		"Successfully retrieved budget rows"
// @Failure		400							{object}	response.ErrorResponse	"Invalid request payload"
// @Failure		500							{object}	response.ErrorResponse	"Failed to get budget data"
// @Router			/budget/ [get]
func (app *application) handleGetExpenseBudget(w http.ResponseWriter, r *http.Request) {
	filter, err := parseBudgetFilter(r)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}

	ctx := r.Context()
	data, err := app.store.ExpenseBudget.GetExpenseBudget(ctx, filter)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "failed to get budget data: "+err.Error())
		return
	}

	resp := &GetBudgetResponse{
		Success: true,
		Data:    data,
		Message: "Successfully retrieved budget rows",
	}

	if err := writeJSON(w, http.StatusOK, resp); err != nil {
		writeJSONError(w, http.StatusInternalServerError, "failed to write response")
	}
}

// @Summary		Get aggregated budget summary
// @Description	Returns budget values aggregated by action_code and expense nature (economic_category.expense_group.expense_element). Accepts multiple subordinate agency codes to accumulate institutions.
// @Tags			Budget
// @Produce		json
// @Param			subordinate_agency_codes	query		string						true	"Comma-separated subordinate agency codes (e.g. 26421 or 26421,26415)"
// @Param			exercise					query		int							false	"Exercise year for filtering (e.g. 2025, optional)"
// @Success		200							{object}	GetBudgetSummaryResponse	"Successfully retrieved budget summary"
// @Failure		400							{object}	response.ErrorResponse		"Invalid request payload"
// @Failure		500							{object}	response.ErrorResponse		"Failed to get budget summary"
// @Router			/budget/summary [get]
func (app *application) handleGetExpenseBudgetSummary(w http.ResponseWriter, r *http.Request) {
	filter, err := parseBudgetFilter(r)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}

	ctx := r.Context()
	data, err := app.store.ExpenseBudget.GetExpenseBudgetSummary(ctx, filter)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "failed to get budget summary: "+err.Error())
		return
	}

	resp := &GetBudgetSummaryResponse{
		Success: true,
		Data:    data,
		Message: "Successfully retrieved budget summary",
	}

	if err := writeJSON(w, http.StatusOK, resp); err != nil {
		writeJSONError(w, http.StatusInternalServerError, "failed to write response")
	}
}

// @Summary		Get global budget summary per action
// @Description	Returns total budget values aggregated by action_code only, collapsing expense nature. Useful for a high-level view per budget action across one or more institutions.
// @Tags			Budget
// @Produce		json
// @Param			subordinate_agency_codes	query		string							true	"Comma-separated subordinate agency codes (e.g. 26421 or 26421,26415)"
// @Param			exercise					query		int								false	"Exercise year for filtering (e.g. 2025, optional)"
// @Success		200							{object}	GetBudgetGlobalSummaryResponse	"Successfully retrieved global budget summary"
// @Failure		400							{object}	response.ErrorResponse			"Invalid request payload"
// @Failure		500							{object}	response.ErrorResponse			"Failed to get global budget summary"
// @Router			/budget/global-summary [get]
func (app *application) handleGetExpenseBudgetGlobalSummary(w http.ResponseWriter, r *http.Request) {
	filter, err := parseBudgetFilter(r)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}

	ctx := r.Context()
	data, err := app.store.ExpenseBudget.GetExpenseBudgetGlobalSummary(ctx, filter)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "failed to get global budget summary: "+err.Error())
		return
	}

	resp := &GetBudgetGlobalSummaryResponse{
		Success: true,
		Data:    data,
		Message: "Successfully retrieved global budget summary",
	}

	if err := writeJSON(w, http.StatusOK, resp); err != nil {
		writeJSONError(w, http.StatusInternalServerError, "failed to write response")
	}
}

func parseBudgetFilter(r *http.Request) (service.BudgetFilter, error) {
	codesParam := r.URL.Query().Get("subordinate_agency_codes")
	if codesParam == "" {
		return service.BudgetFilter{}, fmt.Errorf("subordinate_agency_codes is required")
	}

	var codes []int
	for raw := range strings.SplitSeq(codesParam, ",") {
		code, err := strconv.Atoi(strings.TrimSpace(raw))
		if err != nil {
			return service.BudgetFilter{}, fmt.Errorf("invalid subordinate_agency_code %q: %w", raw, err)
		}
		codes = append(codes, code)
	}

	filter := service.BudgetFilter{SubordinateAgencyCodes: codes}

	if exerciseParam := r.URL.Query().Get("exercise"); exerciseParam != "" {
		exercise, err := strconv.Atoi(exerciseParam)
		if err != nil {
			return service.BudgetFilter{}, fmt.Errorf("invalid exercise: %w", err)
		}
		filter.Exercise = exercise
	}

	return filter, nil
}
