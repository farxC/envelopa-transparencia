package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/farxc/envelopa-transparencia/internal/response"
	"github.com/farxc/envelopa-transparencia/internal/store"
	"github.com/go-playground/validator/v10"
)

var Validate *validator.Validate

func init() {
	Validate = validator.New(validator.WithRequiredStructEnabled())
}

type ExpensesFilterRequest struct {
	ManagementCode      int       `validate:"required,number"`
	ManagementUnitCodes []int     `validate:"dive,number"`
	StartDate           time.Time `validate:"omitempty,ltefield=EndDate"`
	EndDate             time.Time `validate:"omitempty,gtefield=StartDate"`
}

func writeJSON(w http.ResponseWriter, status int, data any) error {
	w.Header().Set("Content-Type", "application/json")

	w.WriteHeader(status)

	return json.NewEncoder(w).Encode(data)
}

func writeJSONError(w http.ResponseWriter, status int, message string) error {
	return writeJSON(w, status, &response.ErrorResponse{Error: message})

}

func readJSON(w http.ResponseWriter, r *http.Request, data any) error {
	maxBytes := 1_048_576 // 1 MB
	r.Body = http.MaxBytesReader(w, r.Body, int64(maxBytes))
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()

	err := dec.Decode(data)
	if err != nil {
		http.Error(w, "Bad request", http.StatusBadRequest)
		return err
	}

	return nil
}
func parseExpensesFilter(r *http.Request) (store.ExpensesFilter, error) {
	var req ExpensesFilterRequest

	// Required: management_code
	managementCode := r.URL.Query().Get("management_code")
	if managementCode == "" {
		return store.ExpensesFilter{}, fmt.Errorf("management_code is required")
	}
	managementCodeInt, err := strconv.Atoi(managementCode)
	if err != nil {
		return store.ExpensesFilter{}, fmt.Errorf("invalid management_code: %w", err)
	}
	req.ManagementCode = managementCodeInt

	// Optional: management_unit_codes
	codesParam := r.URL.Query().Get("management_unit_codes")
	if codesParam != "" {
		req.ManagementUnitCodes = make([]int, 0)
		for _, code := range strings.Split(codesParam, ",") {
			codeInt, err := strconv.Atoi(code)
			if err != nil {
				return store.ExpensesFilter{}, fmt.Errorf("invalid management_unit_code: %w", err)
			}
			req.ManagementUnitCodes = append(req.ManagementUnitCodes, codeInt)
		}
	}

	// Optional: date range
	startParam := r.URL.Query().Get("start_date")
	endParam := r.URL.Query().Get("end_date")

	if startParam != "" {
		startDate, err := time.Parse("2006-01-02", startParam)
		if err != nil {
			return store.ExpensesFilter{}, fmt.Errorf("invalid start_date format (expected YYYY-MM-DD)")
		}
		req.StartDate = startDate
	}

	if endParam != "" {
		endDate, err := time.Parse("2006-01-02", endParam)
		if err != nil {
			return store.ExpensesFilter{}, fmt.Errorf("invalid end_date format (expected YYYY-MM-DD)")
		}
		req.EndDate = endDate
	}

	if err := Validate.Struct(req); err != nil {
		return store.ExpensesFilter{}, fmt.Errorf("validation error: %w", err)
	}

	return req.ToStoreFilter(), nil
}

// Add converter method
func (r ExpensesFilterRequest) ToStoreFilter() store.ExpensesFilter {
	return store.ExpensesFilter{
		ManagementCode:      r.ManagementCode,
		ManagementUnitCodes: r.ManagementUnitCodes,
		StartDate:           r.StartDate,
		EndDate:             r.EndDate,
	}
}
