package main

import (
	"net/http"
	"strings"
	"time"

	"github.com/farxc/envelopa-transparencia/internal/response"
	"github.com/farxc/envelopa-transparencia/internal/store"
)

type GetCommitmentsInformationResponse = response.APIResponse[[]store.CommitmentInformation]

// @Summary		Get commitments information
// @Description	Get commitments information by applying various filters.
// @Tags			Commitments
// @Produce		json
// @Param			start_date				query		string								false	"Start date for filtering (YYYY-MM-DD)"
// @Param			end_date				query		string								false	"End date for filtering (YYYY-MM-DD)"
// @Param			management_code			query		string								false	"Management code for filtering"
// @Param			management_unit_codes	query		string								false	"Comma-separated list of management unit codes for filtering"
// @Param			commitment_codes		query		string								false	"Comma-separated list of commitment codes for filtering"
// @Success		200						{object}	GetCommitmentsInformationResponse	"Successfully retrieved commitment information"
// @Failure		500						{object}	response.ErrorResponse				"Failed to filter commitments table"
// @Router			/commitments [get]
func (app *application) handleGetCommitmentsInformation(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	startParam := r.URL.Query().Get("start_date")
	endParam := r.URL.Query().Get("end_date")
	managementCodeParam := r.URL.Query().Get("management_code")
	managementUnitCodesParam := r.URL.Query().Get("management_unit_codes")
	commitmentCodesParam := r.URL.Query().Get("commitment_codes")

	var filter store.GetCommitmentInformationFilter

	filter.StartDate, _ = time.Parse("2006-01-02", parseDateOrDefault(startParam, "2000-01-01"))
	filter.EndDate, _ = time.Parse("2006-01-02", parseDateOrDefault(endParam, "2100-12-31"))

	if managementCodeParam != "" {
		filter.ManagementCode = managementCodeParam
	}

	if managementUnitCodesParam != "" {
		filter.ManagementUnitCodes = strings.Split(managementUnitCodesParam, ",")
	}

	if commitmentCodesParam != "" {
		filter.CommitmentCodes = strings.Split(commitmentCodesParam, ",")
	}

	response := &GetCommitmentsInformationResponse{}

	data, err := app.store.Commitment.GetCommitmentInformation(ctx, filter)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "failed to filter commitments table: "+err.Error())
		return
	}

	response.Data = data
	response.Success = true
	response.Message = "Successfully retrieved commitment information"

	if err := writeJSON(w, http.StatusOK, response); err != nil {
		writeJSONError(w, http.StatusInternalServerError, "failed to write response")
	}
}
