package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/farxc/envelopa-transparencia/internal/db"
	"github.com/farxc/envelopa-transparencia/internal/env"
	"github.com/farxc/envelopa-transparencia/internal/logger"
	"github.com/farxc/envelopa-transparencia/internal/store"
	"github.com/farxc/envelopa-transparencia/internal/transparency"
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

func createDirIfNotExist(dirPath string) error {
	if _, err := os.Stat(dirPath); os.IsNotExist(err) {
		err := os.Mkdir(dirPath, os.ModePerm)
		if err != nil {
			return err
		}
	}
	return nil
}

func createTmpDirs(appLogger *logger.Logger) error {
	const component = "TempDirCreator"
	dirs := []string{"tmp", "tmp/zips", "tmp/data"}
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
	const component = "Main"
	monitor := NewMonitor()
	appLogger := &logger.Logger{MinLevel: logger.LevelInfo}

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
	ctx := context.Background()

	yesterday := time.Now().AddDate(0, 0, -1).Format("2006-01-02")
	initDatePtr := flag.String("init", yesterday, "Initial date for data extraction")
	endDatePtr := flag.String("end", yesterday, "End date for data extraction")
	byManagingCodePtr := flag.Bool("byManagingCode", false, "Extract data by managing code or managing unit code")
	triggerPtr := flag.String("trigger", "MANUAL", "Trigger source: MANUAL, SCHEDULED")
	codesPtr := flag.String("codes", "158454,158148,158341,158342,158343,158345,158376,158332,158533,158635,158636", "Comma-separated list of Unit Codes to extract")
	logLevelPtr := flag.String("loglevel", "info", "Log level: debug, info, warn, error")
	concurrencyPtr := flag.Int("concurrency", 10, "Number of concurrent workers")
	flag.Parse()

	// Set log level based on flag
	switch strings.ToLower(*logLevelPtr) {
	case "debug":
		appLogger.SetLogLevel(logger.LevelDebug)
	case "info":
		appLogger.SetLogLevel(logger.LevelInfo)
	case "warn":
		appLogger.SetLogLevel(logger.LevelWarn)
	case "error":
		appLogger.SetLogLevel(logger.LevelError)
	default:
		appLogger.SetLogLevel(logger.LevelInfo)
	}

	init_date := *initDatePtr
	end_date := *endDatePtr
	codes := strings.Split(*codesPtr, ",")
	isManagingCode := *byManagingCodePtr

	codesArr := []int64{}
	for _, c := range codes {
		var codeInt int64
		fmt.Sscanf(c, "%d", &codeInt)
		codesArr = append(codesArr, codeInt)
	}

	appLogger.Info(component, "Application started: initDate=%s endDate=%s codesCount=%d logLevel=%s", init_date, end_date, len(codes), *logLevelPtr)

	// Create necessary directories
	err = createTmpDirs(appLogger)
	if err != nil {
		appLogger.Fatal(component, "Failed to create temporary directories: error=%v", err)
		return
	}

	init_parsed_date, err := time.Parse(time.DateOnly, init_date)
	if err != nil {
		appLogger.Fatal(component, "Invalid init date format: date=%s error=%v", init_date, err)
		return
	}
	end_parsed_date, err := time.Parse(time.DateOnly, end_date)
	if err != nil {
		appLogger.Fatal(component, "Invalid end date format: date=%s error=%v", end_date, err)
		return
	}

	err = createDirIfNotExist("output")
	if err != nil {
		appLogger.Fatal(component, "Failed to create output directory: error=%v", err)
		return
	}

	// Initialize Orchestrator
	orchestrator := transparency.NewOrchestrator(storage, appLogger, *concurrencyPtr)

	// 1. Sync state
	err = orchestrator.InitializeState(ctx, init_parsed_date, end_parsed_date, codesArr)
	if err != nil {
		appLogger.Fatal(component, "Failed to initialize orchestrator state: error=%v", err)
		return
	}

	// 2. Start workers
	orchestrator.Start(ctx)

	// 3. Queue jobs
	currentDate := init_parsed_date
	for !currentDate.After(end_parsed_date) {
		if orchestrator.ShouldProcess(currentDate) {
			orchestrator.AddJob(transparency.IngestionJob{
				Date:           currentDate,
				Codes:          codesArr,
				IsManagingCode: isManagingCode,
				Trigger:        *triggerPtr,
			})
		} else {
			appLogger.Info(component, "Skipping date (already processed or active): date=%s", currentDate.Format(time.DateOnly))
		}
		currentDate = currentDate.AddDate(0, 0, 1)
	}

	// 4. Wait for completion
	orchestrator.Close()
	orchestrator.Wait()

	timeTaken := time.Since(starting_time)
	appLogger.Info(component, "Application completed successfully: duration=%.2f minutes", timeTaken.Minutes())
}
