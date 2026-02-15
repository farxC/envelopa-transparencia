package main

import (
	"net/http"
	"strconv"

	"github.com/farxc/envelopa-transparencia/internal/response"
	"github.com/farxc/envelopa-transparencia/internal/store"
)

type GetIngestionHistoryResponse = response.APIResponse[[]store.IngestionHistory]
type CreateIngestionResponse = response.APIResponse[*store.IngestionHistory]

// @Summary		Get ingestion history
// @Description	Get a list of the latest ingestion records.
// @Tags			Ingestion
// @Produce		json
// @Param			limit	query		int							false	"Limit the number of results"	default(10)
// @Success		200		{object}	GetIngestionHistoryResponse	"Successfully retrieved latest ingestion records"
// @Failure		500		{object}	response.ErrorResponse		"Failed to get ingestion history"
// @Router			/ingestion/history [get]
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

// @Summary		Create ingestion record
// @Description	Creates a new ingestion record with IN_PROGRESS status.
// @Tags			Ingestion
// @Accept			json
// @Produce		json
// @Param			ingestion	body		object{reference_date:string,source_file:string,trigger_type:string,scope_type:string,processed_codes:[]int64}	true	"Ingestion record details"
// @Success		201			{object}	CreateIngestionResponse																							"Ingestion record initialized"
// @Failure		400			{object}	response.ErrorResponse																							"Invalid request payload or missing fields"
// @Failure		500			{object}	response.ErrorResponse																							"Failed to create ingestion record"
// @Router			/ingestion [post]
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
