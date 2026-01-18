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
	"github.com/farxc/transparency_wrapper/internal/transparency/downloader"
	"github.com/farxc/transparency_wrapper/internal/transparency/files"
	"github.com/farxc/transparency_wrapper/internal/transparency/types"
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

func clearTmpData(appLogger *logger.Logger) {
	const component = "TempCleanerData"
	err := os.RemoveAll("tmp/data")
	if err != nil {
		appLogger.Warn(component, "Failed to clear temp dir: dir=%s error=%v", "tmp/data", err)
	} else {
		appLogger.Info(component, "Temp dir cleared: dir=%s", "tmp/data")
	}

}

func isFileAlreadyDownloaded(date string) (exists bool, expectedPath string) {
	filePath := fmt.Sprintf("tmp/zips/despesas_%s.zip", date)
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		return false, filePath
	}
	return true, filePath
}

func DonwloadData(init_parsed_date, end_parsed_date time.Time, appLogger *logger.Logger) ([]types.OutputExtractionFiles, error) {
	var extractions []types.OutputExtractionFiles
	var wg sync.WaitGroup
	var mu sync.Mutex
	component := "Downloader"
	var url string

	appLogger.Info(component, "Starting download phase")
	downloadCount := 0
	for !init_parsed_date.After(end_parsed_date) {
		date := init_parsed_date.Format("20060102")
		url = downloader.PortalTransparenciaURL + date
		wg.Add(1)
		downloadCount++
		go func(u, d string) {
			defer wg.Done()
			var path string
			var extraction files.ExtractionResult
			exists, path := isFileAlreadyDownloaded(d)

			if !exists {
				download := downloader.FetchData(u, d, appLogger)
				if !download.Success {
					appLogger.Warn(component, "Download failed: date=%s", d)
					return
				}
				path = download.OutputPath
			}

			extraction = files.UnzipFile(path, "tmp/data/despesas_"+d, appLogger)

			if !extraction.Success {
				appLogger.Warn(component, "Extraction failed: date=%s", d)
				return
			}

			extractedFiles := files.BuildFilesForDate(d, extraction.OutputDir)

			mu.Lock()
			extractions = append(extractions, types.OutputExtractionFiles{Date: d, Files: extractedFiles})
			mu.Unlock()
			appLogger.Info(component, "Day extraction ready: date=%s outputDir=%s", date, extraction.OutputDir)
		}(url, date)
		init_parsed_date = init_parsed_date.AddDate(0, 0, 1)
	}

	appLogger.Info(component, "Waiting for downloads to complete: totalDays=%d", downloadCount)
	wg.Wait()

	return extractions, nil
}

func ProcessExtractions(extractions []types.OutputExtractionFiles, ugs []string, appLogger *logger.Logger, commitmentsOnly bool) (bool, error) {
	const component = "DataProcessor"
	MAX_CONCURRENT_EXTRACTIONS_DATA := 7
	extractions_semaphore := make(chan struct{}, MAX_CONCURRENT_EXTRACTIONS_DATA)
	var wg sync.WaitGroup
	appLogger.Info(component, "Starting data processing phase: extractionsReady=%d maxConcurrent=%d", len(extractions), MAX_CONCURRENT_EXTRACTIONS_DATA)

	for _, extraction := range extractions {
		wg.Add(1)
		go func(ex types.OutputExtractionFiles) {
			defer wg.Done()
			extractions_semaphore <- struct{}{}
			defer func() { <-extractions_semaphore }()

			appLogger.Debug(component, "Processing extraction: date=%s", ex.Date)
			var payload *types.CommitmentPayload
			var err error

			//Memory intensive
			if !commitmentsOnly {
				payload, err = transparency.ExtractData(ex, ugs, appLogger)
			} else {
				//Less memory intensive
				payload, err = transparency.BuildCommitmentPayload(ex, ugs)
			}

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
				return
			}
			appLogger.Info(component, "Output file written: date=%s path=%s size=%d bytes", ex.Date, outputPath, len(jsonData))
		}(extraction)
	}
	wg.Wait()
	return true, nil
}

func main() {
	const component = "Main"
	monitor := NewMonitor()
	var appLogger = &logger.Logger{MinLevel: logger.LevelInfo}

	monitor.Start(400*time.Millisecond, appLogger)

	// Configure log output format
	log.SetFlags(0) // Remove default timestamp since we add our own

	starting_time := time.Now()
	appLogger.Info(component, "Application starting: startTime=%s", starting_time.Format(time.RFC3339))
	yesterday := time.Now().AddDate(0, 0, -1).Format("2006-01-02")
	initDatePtr := flag.String("init", yesterday, "Initial date for data extraction")
	endDatePtr := flag.String("end", yesterday, "End date for data extraction")
	ugsPtr := flag.String("ugs", "158454,158148,158341,158342,158343,158345,158376,158332,158533,158635,158636", "Comma-separated list of Unit Codes to extract")
	commitmentsOnlyPtr := flag.Bool("commitmentsOnly", true, "Process only commitments")
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
	err := createTmpDirs(appLogger)

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

	extractions, err := DonwloadData(init_parsed_date, end_parsed_date, appLogger)
	if err != nil {
		appLogger.Fatal(component, "Data download failed: error=%v", err)
		return
	}

	ok, err := ProcessExtractions(extractions, ugs, appLogger, *commitmentsOnlyPtr)
	if err != nil || !ok {
		appLogger.Fatal(component, "Data processing failed: error=%v", err)
		return
	}

	// clearTempDirs(appLogger)

	timeTaken := time.Since(starting_time)
	appLogger.Info(component, "Application completed successfully: duration=%.2f seconds", timeTaken.Seconds())
}