package main

import (
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/farxc/envelopa-transparencia/docs"
	"github.com/farxc/envelopa-transparencia/internal/store"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	httpSwagger "github.com/swaggo/http-swagger"
)

type application struct {
	config config
	store  store.Storage
}

type config struct {
	addr   string
	apiUrl string
	db     dbConfig
}

type dbConfig struct {
	addr         string
	maxOpenConns int
	maxIdleConns int
	maxIdleTime  string
}

func (app *application) mount() http.Handler {
	r := chi.NewRouter()

	r.Use(middleware.Recoverer)
	r.Use(middleware.Logger)
	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)

	// Set a timeout value on the request context (ctx), that will signal
	// through ctx.Done() that the request has timed out and further
	// processing should be stopped.
	r.Use(middleware.Timeout(60 * time.Second))

	r.Route("/v1", func(r chi.Router) {
		r.Get("/health", app.healthCheckHandler)

		docsURL := fmt.Sprintf("%s/docs/doc.json", app.config.addr)
		r.Get("/docs/*", httpSwagger.Handler(
			httpSwagger.URL(docsURL),
		))

		r.Route("/expenses", func(r chi.Router) {
			r.Get("/summary", app.handleGetExpensesSummary)
			r.Get("/summary/by-management", app.handleGetExpensesSummaryByManagement)
			r.Get("/budget-execution/report", app.handleGetBudgetExecutionReport)
			r.Get("/top-favored", app.handleGetTopFavored)
		})
		r.Route("/commitments", func(r chi.Router) {
			r.Get("/", app.handleGetCommitmentsInformation)
		})
		r.Route("/ingestion", func(r chi.Router) {
			r.Get("/history", app.handleGetIngestionHistory)
			r.Post("/", app.handleCreateIngestion)
		})
	})

	return r
}

func (app *application) run(mux http.Handler) error {
	version := "1.0.0"
	// Docs
	docs.SwaggerInfo.Version = version
	docs.SwaggerInfo.Host = app.config.apiUrl
	docs.SwaggerInfo.BasePath = "/v1"

	srv := &http.Server{
		Addr:         app.config.addr,
		Handler:      mux,
		WriteTimeout: time.Second * 120,
		ReadTimeout:  time.Second * 40,
		IdleTimeout:  time.Minute,
	}

	log.Printf("Server started on %s", app.config.addr)
	return srv.ListenAndServe()
}
