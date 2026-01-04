package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/farxc/transparency_wrapper/internal/logger"
	"github.com/farxc/transparency_wrapper/internal/transparency"
)

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

func createDirsIfNotExist(dirPath string) error {
	if _, err := os.Stat(dirPath); os.IsNotExist(err) {
		err := os.MkdirAll(dirPath, os.ModePerm)
		if err != nil {
			return err
		}
	}
	return nil
}

func clearTempDirs(appLogger *logger.Logger) {
	const component = "TempCleaner"
	dirs := []string{"tmp/zips", "tmp/data"}
	for _, dir := range dirs {
		err := os.RemoveAll(dir)
		if err != nil {
			appLogger.Warn(component, "Failed to clear temp dir: dir=%s error=%v", dir, err)
		} else {
			appLogger.Info(component, "Temp dir cleared: dir=%s", dir)
		}
	}
}

func main() {
	const component = "Main"
	monitor := NewMonitor()
	var appLogger = &logger.Logger{MinLevel: logger.LevelInfo}

	monitor.Start(400*time.Millisecond, appLogger)

	// Configure log output format
	log.SetFlags(0) // Remove default timestamp since we add our own

	var url string
	yesterday := time.Now().AddDate(0, 0, -1).Format("2006-01-02")
	initDatePtr := flag.String("init", yesterday, "Initial date for data extraction")
	endDatePtr := flag.String("end", yesterday, "End date for data extraction")
	ugsPtr := flag.String("ugs", "158454,158148,158341,158342,158343,158345,158376,158332,158533,158635,158636", "Comma-separated list of Unit Codes to extract")
	logLevelPtr := flag.String("loglevel", "info", "Log level: debug, info, warn, error")
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
	ugs := strings.Split(*ugsPtr, ",")

	appLogger.Info(component, "Application started: initDate=%s endDate=%s ugsCount=%d logLevel=%s", init_date, end_date, len(ugs), *logLevelPtr)

	// Create necessary directories
	dirs := []string{"tmp/zips", "tmp/data"}
	MAX_CONCURRENT_EXTRACTIONS_DATA := 7
	extractions_semaphore := make(chan struct{}, MAX_CONCURRENT_EXTRACTIONS_DATA)

	appLogger.Debug(component, "Creating required directories: dirs=%v", dirs)
	for _, dir := range dirs {
		err := createDirsIfNotExist(dir)
		if err != nil {
			appLogger.Fatal(component, "Failed to create directory: dir=%s error=%v", dir, err)
		}
	}

	init_parsed_date, err := time.Parse(time.DateOnly, init_date)
	if err != nil {
		appLogger.Fatal(component, "Invalid init date format: date=%s error=%v", init_date, err)
	}
	end_parsed_date, err := time.Parse(time.DateOnly, end_date)
	if err != nil {
		appLogger.Fatal(component, "Invalid end date format: date=%s error=%v", end_date, err)
	}

	var extractions []transparency.DayExtraction

	var mu sync.Mutex

	var wg sync.WaitGroup

	appLogger.Info(component, "Starting download phase")
	downloadCount := 0
	for !init_parsed_date.After(end_parsed_date) {
		init_date = init_parsed_date.Format("20060102")
		url = transparency.PORTAL_TRANSPARENCIA_URL + init_date
		wg.Add(1)
		downloadCount++
		go func(u, d string) {
			defer wg.Done()
			download := transparency.FetchData(u, d, appLogger)
			var extraction transparency.ExtractionResult

			if !download.Success {
				appLogger.Warn(component, "Download failed: date=%s", d)
				return
			}

			if download.OutputPath != "" {
				extraction = transparency.UnzipFile(download.OutputPath, "tmp/data/despesas_"+d, appLogger)
			}

			if !extraction.Success {
				appLogger.Warn(component, "Extraction failed: date=%s", d)
				return
			}

			files := transparency.BuildFilesForDate(d, extraction.OutputDir)

			mu.Lock()
			extractions = append(extractions, transparency.DayExtraction{Date: d, Files: files})
			mu.Unlock()
			appLogger.Info(component, "Day extraction ready: date=%s outputDir=%s", d, extraction.OutputDir)

		}(url, init_date)
		init_parsed_date = init_parsed_date.AddDate(0, 0, 1)
	}
	appLogger.Info(component, "Waiting for downloads to complete: totalDays=%d", downloadCount)
	wg.Wait()

	// Create a goroutine for each day
	createDirsIfNotExist("output")
	appLogger.Info(component, "Starting data processing phase: extractionsReady=%d maxConcurrent=%d", len(extractions), MAX_CONCURRENT_EXTRACTIONS_DATA)

	for _, extraction := range extractions {
		wg.Add(1)
		go func(ex transparency.DayExtraction) {
			defer wg.Done()
			extractions_semaphore <- struct{}{}
			defer func() { <-extractions_semaphore }()

			appLogger.Debug(component, "Processing extraction: date=%s", ex.Date)

			//Memory intensive
			payload, err := transparency.ExtractData([]transparency.DayExtraction{ex}, ugs, appLogger)
			if err != nil {
				appLogger.Warn(component, "Data extraction skipped: date=%s reason=%v", ex.Date, err)
				return
			}

			jsonData, err := json.Marshal(payload)
			if err != nil {
				appLogger.Error(component, "JSON marshaling failed: date=%s error=%v", ex.Date, err)
				return
			}

			outputPath := fmt.Sprintf("output/extraction_%s.json", ex.Date)
			if err := os.WriteFile(outputPath, jsonData, 0644); err != nil {
				appLogger.Error(component, "Output file write failed: date=%s path=%s error=%v", ex.Date, outputPath, err)
			} else {
				appLogger.Info(component, "Output file written: date=%s path=%s size=%d bytes", ex.Date, outputPath, len(jsonData))
			}

		}(extraction)
	}

	// clearTempDirs(appLogger)

	wg.Wait()
	appLogger.Info(component, "Application completed successfully")
}
