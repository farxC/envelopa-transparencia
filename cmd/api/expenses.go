package main

import (
	"fmt"
	"net/http"
	"time"

	"github.com/farxc/transparency_wrapper/internal/store"
)

func (app *application) handleFilterExpensesTable(w http.ResponseWriter, r *http.Request) {
	startParam := r.URL.Query().Get("start_date")
	endParam := r.URL.Query().Get("end_date")
	scopeTypeParam := r.URL.Query().Get("scope_type")
	codeParam := r.URL.Query().Get("code")

	fmt.Println(startParam)
	fmt.Println(endParam)
	fmt.Println(scopeTypeParam)
	fmt.Println(codeParam)

	var filter store.ExpensesFilter

	filter.StartDate, _ = time.Parse("2006-01-02", parseDateOrDefault(startParam, "2000-01-01"))
	filter.EndDate, _ = time.Parse("2006-01-02", parseDateOrDefault(endParam, "2100-12-31"))
	filter.Code = parseIntOrDefault(codeParam, 0)
	filter.ScopeType = store.ScopeType(scopeTypeParam)

	ctx := r.Context()
	fmt.Println(ctx)
	data, err := app.store.Expenses.FilterExpensesTable(ctx, filter)
	fmt.Printf("%+v\n", data)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "failed to filter expenses table: "+err.Error())
		return
	}

	if err := writeJSON(w, http.StatusOK, data); err != nil {
		writeJSONError(w, http.StatusInternalServerError, "failed to write response")
	}
}
