package main

import (
	"net/http"
	"strconv"

	"github.com/farxc/envelopa-transparencia/internal/response"
	"github.com/farxc/envelopa-transparencia/internal/store"
)

type GetIngestionHistoryResponse = response.APIResponse[[]store.IngestionHistory]
type CreateIngestionResponse = response.APIResponse[*store.IngestionHistory]

func (app *application) handleGetIngestionHistory(w http.ResponseWriter, r *http.Request) {
	limitParam := r.URL.Query().Get("limit")
	limit := 10
	if limitParam != "" {
		if l, err := strconv.Atoi(limitParam); err == nil {
			limit = l
		}
	}

	ctx := r.Context()
	data, err := app.store.IngestionHistory.GetLatest(ctx, limit)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "failed to get ingestion history: "+err.Error())
		return
	}

	response := &GetIngestionHistoryResponse{
		Success: true,
		Data:    data,
		Message: "Successfully retrieved latest ingestion records",
	}

	if err := writeJSON(w, http.StatusOK, response); err != nil {
		writeJSONError(w, http.StatusInternalServerError, "failed to write response")
	}
}

func (app *application) handleCreateIngestion(w http.ResponseWriter, r *http.Request) {
	var input struct {
		ReferenceDate  string  `json:"reference_date"`
		SourceFile     string  `json:"source_file"`
		TriggerType    string  `json:"trigger_type"`
		ScopeType      string  `json:"scope_type"`
		ProcessedCodes []int64 `json:"processed_codes"`
	}

	if err := readJSON(w, r, &input); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid request payload")
		return
	}

	// Basic validation (can be expanded)
	if input.ReferenceDate == "" || input.TriggerType == "" || input.ScopeType == "" {
		writeJSONError(w, http.StatusBadRequest, "missing required fields")
		return
	}

	// Use parseDateOrDefault logic if needed, but here we expect a specific format
	refDate, err := parseTime(input.ReferenceDate)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid reference_date format (YYYY-MM-DD expected)")
		return
	}

	history := &store.IngestionHistory{
		ReferenceDate:  refDate,
		SourceFile:     input.SourceFile,
		TriggerType:    input.TriggerType,
		ScopeType:      input.ScopeType,
		Status:         store.StatusInProgress,
		ProcessedCodes: input.ProcessedCodes,
	}

	ctx := r.Context()
	if err := app.store.IngestionHistory.InsertIngestionHistory(ctx, history); err != nil {
		writeJSONError(w, http.StatusInternalServerError, "failed to create ingestion record: "+err.Error())
		return
	}

	response := &CreateIngestionResponse{
		Success: true,
		Data:    history,
		Message: "Ingestion record initialized with IN_PROGRESS status",
	}

	if err := writeJSON(w, http.StatusCreated, response); err != nil {
		writeJSONError(w, http.StatusInternalServerError, "failed to write response")
	}
}

func (app *application) handleUpdateIngestionStatus(w http.ResponseWriter, r *http.Request) {
	idParam := r.URL.Query().Get("id")
	id, err := strconv.ParseInt(idParam, 10, 64)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid record id")
		return
	}

	var input struct {
		Status string `json:"status"`
	}

	if err := readJSON(w, r, &input); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid request payload")
		return
	}

	// Validate status
	if input.Status != store.StatusSuccess && input.Status != store.StatusFailure && input.Status != store.StatusPartial {
		writeJSONError(w, http.StatusBadRequest, "invalid status value")
		return
	}

	ctx := r.Context()
	if err := app.store.IngestionHistory.UpdateIngestionStatus(ctx, id, input.Status); err != nil {
		writeJSONError(w, http.StatusInternalServerError, "failed to update ingestion status: "+err.Error())
		return
	}

	if err := writeJSON(w, http.StatusOK, map[string]string{"message": "status updated successfully"}); err != nil {
		writeJSONError(w, http.StatusInternalServerError, "failed to write response")
	}
}
