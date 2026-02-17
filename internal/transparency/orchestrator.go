package transparency

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/farxc/envelopa-transparencia/internal/logger"
	"github.com/farxc/envelopa-transparencia/internal/store"
	"github.com/farxc/envelopa-transparencia/internal/transparency/downloader"
	"github.com/farxc/envelopa-transparencia/internal/transparency/files"
	"github.com/farxc/envelopa-transparencia/internal/transparency/load"
	"github.com/farxc/envelopa-transparencia/internal/transparency/types"
)

type IngestionJob struct {
	Date           time.Time
	Codes          []int64
	Attempt        int
	IsManagingCode bool
	Trigger        string
	SourceFile     string
}

type IngestionResult struct {
	Job   IngestionJob
	ID    int64
	Error error
}

type Orchestrator struct {
	storage   *store.Storage
	appLogger *logger.Logger

	// Settings
	maxConcurrency int
	retryLimit     int
	staleTimeout   time.Duration

	// Internal State
	statusMap map[string]store.IngestionHistory
	mu        sync.RWMutex
	wg        sync.WaitGroup

	// Channels
	jobChan    chan IngestionJob
	resultChan chan IngestionResult
}

func NewOrchestrator(storage *store.Storage, appLogger *logger.Logger, concurrency int) *Orchestrator {
	return &Orchestrator{
		storage:        storage,
		appLogger:      appLogger,
		maxConcurrency: concurrency,
		retryLimit:     3,
		staleTimeout:   30 * time.Minute,
		statusMap:      make(map[string]store.IngestionHistory),
		jobChan:        make(chan IngestionJob, 100),
		resultChan:     make(chan IngestionResult, 100),
	}
}

// InitializeState performs the bulk check to identify what needs to be processed
func (o *Orchestrator) InitializeState(ctx context.Context, startDate, endDate time.Time, codes []int64) error {
	const component = "Orchestrator-Init"
	o.appLogger.Info(component, "Syncing initial state from database: range=%s to %s", startDate.Format(time.DateOnly), endDate.Format(time.DateOnly))

	history, err := o.storage.IngestionHistory.GetHistoryInRange(ctx, startDate, endDate, codes)
	if err != nil {
		return fmt.Errorf("failed to load history: %w", err)
	}

	o.mu.Lock()
	defer o.mu.Unlock()

	for _, h := range history {
		dateKey := h.ReferenceDate.Format(time.DateOnly)
		// We only care about the latest record for each date in the map
		if existing, ok := o.statusMap[dateKey]; !ok || h.ProcessedAt.After(existing.ProcessedAt) {
			o.statusMap[dateKey] = h
		}
	}

	o.appLogger.Info(component, "State sync complete: uniqueDatesFound=%d", len(o.statusMap))
	return nil
}

func (o *Orchestrator) shouldSkip(err error, job IngestionJob) bool {
	return err.Error() == fmt.Sprintf("no matching data found for extraction date %s", job.Date.Format("2006-01-02")) || err.Error() == "dataframe is empty"
}

func (o *Orchestrator) ShouldProcess(date time.Time) bool {
	dateKey := date.Format(time.DateOnly)
	o.mu.RLock()
	defer o.mu.RUnlock()

	h, ok := o.statusMap[dateKey]
	if !ok {
		return true // Never tried
	}

	if h.Status == store.StatusInProgress {
		isLaterThanTimeout := time.Since(h.ProcessedAt) > o.staleTimeout
		return isLaterThanTimeout
	}

	if h.Status == store.StatusSkipped {
		return false // Explicitly marked as skipped
	}

	if h.Status == store.StatusSuccess {
		return false // Already done
	}

	return true
}

func (o *Orchestrator) Start(ctx context.Context) {
	const component = "Orchestrator"
	o.appLogger.Info(component, "Starting orchestrator: concurrency=%d", o.maxConcurrency)

	// 1. Start Workers
	for i := 0; i < o.maxConcurrency; i++ {
		o.wg.Add(1)
		go o.worker(ctx, &o.wg)
	}

	// 2. Start Result Listener (The Feedback Loop)
	go o.listenToResults()
}

func (o *Orchestrator) Wait() {
	o.wg.Wait()
	close(o.resultChan)
}

func (o *Orchestrator) AddJob(job IngestionJob) {
	o.jobChan <- job
}

func (o *Orchestrator) Close() {
	close(o.jobChan)
}

func (o *Orchestrator) worker(ctx context.Context, wg *sync.WaitGroup) {
	const component = "Worker"
	defer wg.Done()

	for job := range o.jobChan {
		dateStr := job.Date.Format(time.DateOnly)
		o.appLogger.Debug(component, "Processing job: date=%s attempt=%d", dateStr, job.Attempt)

		// A. Create IN_PROGRESS record
		scope := store.ScopeTypeManagingUnit
		if job.IsManagingCode {
			scope = store.ScopeTypeManagement
		}

		history := &store.IngestionHistory{
			ReferenceDate:  job.Date,
			TriggerType:    job.Trigger,
			ScopeType:      scope,
			Status:         store.StatusInProgress,
			SourceFile:     fmt.Sprintf("despesas_%s.zip", job.Date.Format("20060102")),
			ProcessedCodes: job.Codes,
		}

		err := o.storage.IngestionHistory.InsertIngestionHistory(ctx, history)
		if err != nil {
			o.appLogger.Error(component, "Failed to create IN_PROGRESS record: date=%s err=%v", dateStr, err)
			o.resultChan <- IngestionResult{Job: job, Error: err}
			continue
		}

		// B. Perform Extraction & Load
		result := o.processDay(ctx, job)
		result.ID = history.ID

		// C. Finalize Status
		status := store.StatusSuccess
		if result.Error != nil {
			if o.shouldSkip(result.Error, job) {
				status = store.StatusSkipped
			}
			status = store.StatusFailure
		}

		err = o.storage.IngestionHistory.UpdateIngestionStatus(ctx, history.ID, status)
		if err != nil {
			o.appLogger.Error(component, "Failed to update final status: id=%d status=%s err=%v", history.ID, status, err)
		}

		o.resultChan <- result
	}
}

func (o *Orchestrator) processDay(ctx context.Context, job IngestionJob) IngestionResult {
	const component = "Processor"
	dateCode := job.Date.Format("20060102")

	// 1. Download if not downloaded
	expected_path := "tmp/zips/despesas_" + dateCode + ".zip"
	if _, err := os.Stat(expected_path); os.IsNotExist(err) {
		url := downloader.PortalTransparenciaURL + dateCode
		download := downloader.FetchData(url, dateCode, o.appLogger)
		if !download.Success {
			return IngestionResult{Job: job, Error: fmt.Errorf("download failed")}
		}
	}

	// 2. Extract
	outputDir := "tmp/data/despesas_" + dateCode
	extraction := files.UnzipFile(expected_path, outputDir, o.appLogger)
	if !extraction.Success {
		return IngestionResult{Job: job, Error: fmt.Errorf("extraction failed")}
	}
	defer os.RemoveAll(outputDir) // Cleanup extracted CSVs

	// 3. Transform & Load
	codeStrings := make([]string, len(job.Codes))
	for i, c := range job.Codes {
		codeStrings[i] = fmt.Sprintf("%d", c)
	}

	extFiles := types.OutputExtractionFiles{
		Date:  dateCode,
		Files: files.BuildFilesForDate(dateCode, extraction.OutputDir),
	}

	payload, err := ExtractData(extFiles, codeStrings, job.IsManagingCode, o.appLogger)
	if err != nil {

		return IngestionResult{Job: job, Error: err}
	}

	err = load.LoadPayload(ctx, payload, o.storage, o.appLogger)
	if err != nil {
		return IngestionResult{Job: job, Error: err}
	}

	return IngestionResult{Job: job, Error: nil}
}

func (o *Orchestrator) listenToResults() {
	const component = "Orchestrator-Feedback"
	for result := range o.resultChan {
		dateStr := result.Job.Date.Format(time.DateOnly)

		if result.Error != nil {
			if result.Job.Attempt < o.retryLimit && !o.shouldSkip(result.Error, result.Job) {
				o.appLogger.Warn(component, "Job failed, queuing for retry: date=%s attempt=%d err=%v", dateStr, result.Job.Attempt, result.Error)
				result.Job.Attempt++
				o.AddJob(result.Job)
			} else if o.shouldSkip(result.Error, result.Job) {
				o.appLogger.Info(component, "Job marked as skipped: date=%s err=%v", dateStr, result.Error)
			} else {
				o.appLogger.Error(component, "Job failed after max retries: date=%s err=%v", dateStr, result.Error)
			}
		} else {
			o.appLogger.Info(component, "Job completed successfully: date=%s", dateStr)

			o.mu.Lock()
			o.statusMap[dateStr] = store.IngestionHistory{
				Status:        store.StatusSuccess,
				ReferenceDate: result.Job.Date,
				ProcessedAt:   time.Now(),
			}
			o.mu.Unlock()
		}
	}
}
