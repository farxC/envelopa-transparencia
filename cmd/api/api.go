package main

import (
	"log"
	"net/http"
	"time"

	"github.com/farxc/envelopa-transparencia/internal/store"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
)

type application struct {
	config config
	store  store.Storage
}

type config struct {
	addr string
	db   dbConfig
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

	r.Get("/", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("Hello world!"))
	})

	r.Route("/v1", func(r chi.Router) {
		r.Get("/health", app.healthCheckHandler)
		r.Route("/expenses", func(r chi.Router) {
			r.Get("/summary", app.handleGetExpensesSummary)
			r.Get("/summary/global", app.handleGetGlobalExpensesSummary)
			r.Get("/budget-execution/report", app.handleGetBudgetExecutionReport)
			r.Get("/top-favored", app.handleGetTopFavored)
			r.Get("/by-category", app.handleGetExpensesByCategory)
		})
		r.Route("/commitments", func(r chi.Router) {
			r.Get("/", app.handleGetCommitmentsInformation)
		})
		r.Route("/ingestion", func(r chi.Router) {
			r.Get("/history", app.handleGetIngestionHistory)
			r.Post("/", app.handleCreateIngestion)
			r.Patch("/{id}/status", app.handleUpdateIngestionStatus)
		})
	})

	return r
}

func (app *application) run(mux http.Handler) error {

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
