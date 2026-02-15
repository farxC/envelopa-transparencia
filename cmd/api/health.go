package main

import "net/http"

// @Summary		Health check
// @Description	returns the status of the service
// @Tags			Health
// @Produce		json
// @Success		200	{object}	map[string]string
// @Router			/health [get]
func (app *application) healthCheckHandler(w http.ResponseWriter, r *http.Request) {

	data := map[string]string{
		"status":  "available",
		"version": "0.0.1",
	}

	if err := writeJSON(w, http.StatusOK, data); err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
	}
}
