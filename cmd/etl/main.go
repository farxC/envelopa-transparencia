package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/farxc/envelopa-transparencia/internal/application"
	"github.com/farxc/envelopa-transparencia/internal/domain/model"
	"github.com/farxc/envelopa-transparencia/internal/infrastructure/client/portal"
	"github.com/farxc/envelopa-transparencia/internal/infrastructure/db"
	"github.com/farxc/envelopa-transparencia/internal/infrastructure/env"
	"github.com/farxc/envelopa-transparencia/internal/infrastructure/logger"
	"github.com/farxc/envelopa-transparencia/internal/infrastructure/store"
)

type config struct {
	db dbConfig
}

type dbConfig struct {
	addr         string
	maxOpenConns int
	maxIdleConns int
	maxIdleTime  string
}

type ProfilerStats struct {
	PeakGoroutines int
	PeakMemoryMB   uint64
}

type MemoryMonitor struct {
	mu    sync.Mutex
	stats ProfilerStats
	stop  chan struct{}
}

func NewMonitor() *MemoryMonitor {
	return &MemoryMonitor{
		stop: make(chan struct{}),
	}
}

func (m *MemoryMonitor) Start(interval time.Duration, log *logger.Logger) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				m.update(log)
			case <-m.stop:
				return
			}
		}
	}()
}

func (m *MemoryMonitor) update(logger *logger.Logger) {
	const component = "Monitor"

	var mStats runtime.MemStats
	runtime.ReadMemStats(&mStats)

	currentGoroutines := runtime.NumGoroutine()
	currentMemoryMB := mStats.Alloc / 1024 / 1024
	m.mu.Lock()
	defer m.mu.Unlock()

	if currentGoroutines > m.stats.PeakGoroutines {
		m.stats.PeakGoroutines = currentGoroutines
	}
	if currentMemoryMB > m.stats.PeakMemoryMB {
		m.stats.PeakMemoryMB = currentMemoryMB
	}

	logger.Debug(component, "goroutines=%d memoryMB=%d peakGoroutines=%d peakMemoryMB=%d", currentGoroutines, currentMemoryMB, m.stats.PeakGoroutines, m.stats.PeakMemoryMB)
}

func (m *MemoryMonitor) Stop() ProfilerStats {
	close(m.stop)
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.stats
}

func createTmpDirs(appLogger *logger.Logger) error {
	const component = "TempDirCreator"
	dirs := []string{"tmp", "tmp/zips", "tmp/data", "tmp/zips/expenses_execution", "tmp/zips/expenses", "tmp/data/expenses_execution", "tmp/data/expenses", "tmp/zips/budget", "tmp/data/budget"}
	for _, dir := range dirs {
		if _, err := os.Stat(dir); os.IsNotExist(err) {
			err := os.Mkdir(dir, os.ModePerm)
			if err != nil {
				return err
			}
		}
	}
	appLogger.Info(component, "Temporary directories created or already exist: dirs=%v", dirs)
	return nil
}

func main() {
	if err := env.Load(); err != nil {
		log.Fatalf("failed to load .env: %v", err)
	}

	flags, err := parseFlags(os.Args[1:], time.Now(), os.Stderr)
	if errors.Is(err, flag.ErrHelp) {
		os.Exit(0)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n\nRun with -h for usage.\n", err)
		os.Exit(2)
	}

	const component = "Main"
	monitor := NewMonitor()
	appLogger := &logger.Logger{MinLevel: flags.logLevel}

	monitor.Start(400*time.Millisecond, appLogger)

	// Configure log output format
	log.SetFlags(0) // Remove default timestamp since we add our own

	starting_time := time.Now()
	appLogger.Info(component, "Application starting: startTime=%s", starting_time.Format(time.RFC3339))

	cfg := config{
		db: dbConfig{
			addr:         env.GetString("DB_ADDR", "postgres://admin:helloworld@localhost:5454/transparency_wrapper_db?sslmode=disable"),
			maxOpenConns: env.GetInt("DB_MAX_OPEN_CONNS", 25),
			maxIdleConns: env.GetInt("DB_MAX_IDLE_CONNS", 25),
			maxIdleTime:  env.GetString("DB_MAX_IDLE_TIME", "15m"),
		},
	}

	database, err := db.New(
		cfg.db.addr,
		cfg.db.maxOpenConns,
		cfg.db.maxIdleConns,
		cfg.db.maxIdleTime)
	if err != nil {
		appLogger.Fatal(component, "Database connection failed: error=%v", err)
		return
	}
	defer database.Close()
	appLogger.Info(component, "Database connection pool established")

	storage := store.NewStorage(database)
	loader := store.NewStorageLoader(storage, appLogger)
	ctx := context.Background()

	transparency_portal_client := portal.NewTransparencyClient(appLogger, flags.debug, flags.download)

	appLogger.Info(component, "Application started: kind=%s initDate=%s endDate=%s codes=%v byManagingCode=%t trigger=%s concurrency=%d downloadLimit=%d/%s logLevel=%s debug=%t force=%t",
		flags.kind, flags.initDate.Format(time.DateOnly), flags.endDate.Format(time.DateOnly), flags.codes,
		flags.byManagingCode, flags.trigger, flags.concurrency, flags.download.Limit, flags.download.Window, flags.logLevelName, flags.debug, flags.force)
	if flags.kind == kindBudget && flags.byManagingCode {
		appLogger.Warn(component, "-byManagingCode is ignored by the budget kind")
	}

	// Create necessary directories
	err = createTmpDirs(appLogger)
	if err != nil {
		appLogger.Fatal(component, "Failed to create temporary directories: error=%v", err)
		return
	}

	codesArr := flags.codes
	isManagingCode := flags.byManagingCode
	init_parsed_date := flags.initDate
	end_parsed_date := flags.endDate

	// Initialize and run the orchestrator for the requested extraction kind.
	switch flags.kind {

	case kindExpenses:
		pipeline := application.NewExpensesDailyPipeline(transparency_portal_client, loader, appLogger)
		orch := application.NewOrchestrator(pipeline, storage.IngestionHistory, appLogger, flags.concurrency)

		start, end := pipeline.HistoryRange(init_parsed_date, end_parsed_date)
		if err = orch.InitializeState(ctx, start, end, codesArr); err != nil {
			appLogger.Fatal(component, "Failed to initialize orchestrator state: error=%v", err)
			return
		}

		orch.Start(ctx)

		for d := init_parsed_date; !d.After(end_parsed_date); d = d.AddDate(0, 0, 1) {
			job := model.ExpensesDailyJob{
				Date:           d,
				Codes:          codesArr,
				IsManagingCode: isManagingCode,
				Trigger:        flags.trigger,
			}
			if orch.ShouldRun(job, flags.debug, flags.force) {
				orch.AddJob(job)
			} else {
				appLogger.Info(component, "Skipping date (already processed or active): date=%s", d.Format(time.DateOnly))
			}
		}

		orch.Close()
		orch.Wait()

	case kindExpensesExecution:
		pipeline := application.NewExpensesExecutionPipeline(transparency_portal_client, loader, appLogger)
		orch := application.NewOrchestrator(pipeline, storage.IngestionHistory, appLogger, flags.concurrency)

		start, end := pipeline.HistoryRange(init_parsed_date, end_parsed_date)
		if err = orch.InitializeState(ctx, start, end, codesArr); err != nil {
			appLogger.Fatal(component, "Failed to initialize orchestrator state: error=%v", err)
			return
		}

		orch.Start(ctx)

		startMonth := time.Date(init_parsed_date.Year(), init_parsed_date.Month(), 1, 0, 0, 0, 0, init_parsed_date.Location())
		endMonth := time.Date(end_parsed_date.Year(), end_parsed_date.Month(), 1, 0, 0, 0, 0, end_parsed_date.Location())
		for m := startMonth; !m.After(endMonth); m = m.AddDate(0, 1, 0) {
			job := model.ExpensesExecutionJob{
				Year:           m.Format("2006"),
				Month:          m.Format("01"),
				Codes:          codesArr,
				IsManagingCode: isManagingCode,
				Trigger:        flags.trigger,
			}
			if orch.ShouldRun(job, flags.debug, flags.force) {
				orch.AddJob(job)
			} else {
				appLogger.Info(component, "Skipping month (in progress in another run): month=%s-%s", job.Year, job.Month)
			}
		}

		orch.Close()
		orch.Wait()

	case kindBudget:
		pipeline := application.NewBudgetPipeline(transparency_portal_client, loader, appLogger)
		orch := application.NewOrchestrator(pipeline, storage.IngestionHistory, appLogger, flags.concurrency)

		start, end := pipeline.HistoryRange(init_parsed_date, end_parsed_date)
		if err = orch.InitializeState(ctx, start, end, codesArr); err != nil {
			appLogger.Fatal(component, "Failed to initialize orchestrator state: error=%v", err)
			return
		}

		orch.Start(ctx)

		startYear := time.Date(init_parsed_date.Year(), 1, 1, 0, 0, 0, 0, init_parsed_date.Location())
		endYear := time.Date(end_parsed_date.Year(), 1, 1, 0, 0, 0, 0, end_parsed_date.Location())
		for y := startYear; !y.After(endYear); y = y.AddDate(1, 0, 0) {
			job := model.BudgetJob{
				Year:    y.Format("2006"),
				Codes:   codesArr,
				Trigger: flags.trigger,
			}
			if orch.ShouldRun(job, flags.debug, flags.force) {
				orch.AddJob(job)
			} else {
				appLogger.Info(component, "Skipping year (in progress in another run): year=%s", job.Year)
			}
		}

		orch.Close()
		orch.Wait()

	}

	timeTaken := time.Since(starting_time)
	appLogger.Info(component, "Application completed successfully: duration=%.2f minutes", timeTaken.Minutes())
}
