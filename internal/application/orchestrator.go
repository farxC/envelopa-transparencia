package application

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/farxc/envelopa-transparencia/internal/domain/model"
	"github.com/farxc/envelopa-transparencia/internal/domain/repository"
	"github.com/farxc/envelopa-transparencia/internal/infrastructure/logger"
)

const (
	statusInProgress = "IN_PROGRESS"
	statusSuccess    = "SUCCESS"
	statusFailure    = "FAILURE"
	statusSkipped    = "SKIPPED"
)

// jobEnvelope wraps a job with its retry attempt count.
// The attempt count is tracked internally by the orchestrator
// so job types remain free of orchestration concerns.
type jobEnvelope[J any] struct {
	job     J
	attempt int
}

type jobResult[J any] struct {
	envelope jobEnvelope[J]
	id       int64
	err      error
}

type Orchestrator[J any] struct {
	pipeline    Pipeline[J]
	historyRepo repository.IngestionHistoryInterface
	appLogger   *logger.Logger

	maxConcurrency int
	retryLimit     int
	staleTimeout   time.Duration

	statusMap     map[string]model.IngestionHistory
	mu            sync.RWMutex
	wg            sync.WaitGroup
	listenerWg    sync.WaitGroup
	jobChanClosed bool

	jobChan    chan jobEnvelope[J]
	resultChan chan jobResult[J]
}

func NewOrchestrator[J any](
	pipeline Pipeline[J],
	historyRepo repository.IngestionHistoryInterface,
	appLogger *logger.Logger,
	concurrency int,
) *Orchestrator[J] {
	return &Orchestrator[J]{
		pipeline:       pipeline,
		historyRepo:    historyRepo,
		appLogger:      appLogger,
		maxConcurrency: concurrency,
		retryLimit:     3,
		staleTimeout:   30 * time.Minute,
		statusMap:      make(map[string]model.IngestionHistory),
		jobChan:        make(chan jobEnvelope[J], 100),
		resultChan:     make(chan jobResult[J], 100),
	}
}

// InitializeState loads existing ingestion history from DB to populate the
// in-memory status map, so already-processed jobs are skipped on startup.
func (o *Orchestrator[J]) InitializeState(ctx context.Context, startDate, endDate time.Time, codes []int64) error {
	const component = "Orchestrator-Init"
	start, end := o.pipeline.HistoryRange(startDate, endDate)
	o.appLogger.Info(component, "Syncing state from DB: range=%s to %s", start.Format(time.DateOnly), end.Format(time.DateOnly))

	history, err := o.historyRepo.GetHistoryInRange(ctx, start, end, codes)
	if err != nil {
		return fmt.Errorf("failed to load history: %w", err)
	}

	o.mu.Lock()
	defer o.mu.Unlock()
	for _, h := range history {
		key := o.pipeline.HistoryKey(h)
		if existing, ok := o.statusMap[key]; !ok || h.ProcessedAt.After(existing.ProcessedAt) {
			o.statusMap[key] = h
		}
	}

	o.appLogger.Info(component, "State sync complete: uniqueKeysFound=%d", len(o.statusMap))
	return nil
}

// ShouldProcess reports whether the job identified by key needs to be processed.
func (o *Orchestrator[J]) ShouldProcess(key string) bool {
	o.mu.RLock()
	defer o.mu.RUnlock()

	h, ok := o.statusMap[key]
	if !ok {
		return true
	}
	switch h.Status {
	case statusInProgress:
		return time.Since(h.ProcessedAt) > o.staleTimeout
	case statusSkipped, statusSuccess:
		return false
	default:
		return true
	}
}

func (o *Orchestrator[J]) Start(ctx context.Context) {
	const component = "Orchestrator"
	o.appLogger.Info(component, "Starting orchestrator: concurrency=%d", o.maxConcurrency)
	for i := 0; i < o.maxConcurrency; i++ {
		o.wg.Add(1)
		go o.worker(ctx, &o.wg)
	}
	o.listenerWg.Add(1)
	go o.listenToResults()
}

func (o *Orchestrator[J]) Wait() {
	o.wg.Wait()
	close(o.resultChan)
	o.listenerWg.Wait()
}

func (o *Orchestrator[J]) AddJob(job J) bool {
	o.mu.RLock()
	closed := o.jobChanClosed
	o.mu.RUnlock()
	if closed {
		return false
	}
	o.jobChan <- jobEnvelope[J]{job: job, attempt: 0}
	return true
}

func (o *Orchestrator[J]) Close() {
	o.mu.Lock()
	o.jobChanClosed = true
	o.mu.Unlock()
	close(o.jobChan)
}

func (o *Orchestrator[J]) worker(ctx context.Context, wg *sync.WaitGroup) {
	const component = "Worker"
	defer wg.Done()

	for envelope := range o.jobChan {
		key := o.pipeline.StatusKey(envelope.job)
		o.appLogger.Debug(component, "Processing job: key=%s attempt=%d", key, envelope.attempt)

		// Build and persist the IN_PROGRESS audit record before any ETL work.
		history := o.pipeline.BuildHistoryRecord(envelope.job)
		history.Status = statusInProgress
		if err := o.historyRepo.InsertIngestionHistory(ctx, history); err != nil {
			o.appLogger.Error(component, "Failed to create IN_PROGRESS record: key=%s err=%v", key, err)
			o.resultChan <- jobResult[J]{envelope: envelope, err: err}
			continue
		}

		// Delegate all extraction logic to the pipeline.
		etlErr := o.pipeline.Execute(ctx, envelope.job)

		// Determine final status and update the audit record.
		status := statusSuccess
		if etlErr != nil {
			if o.pipeline.ShouldSkip(etlErr, envelope.job) {
				status = statusSkipped
			} else {
				status = statusFailure
			}
		}
		if err := o.historyRepo.UpdateIngestionStatus(ctx, history.ID, status); err != nil {
			o.appLogger.Error(component, "Failed to update status: id=%d status=%s err=%v", history.ID, status, err)
		}

		o.resultChan <- jobResult[J]{envelope: envelope, id: history.ID, err: etlErr}
	}
}

func (o *Orchestrator[J]) listenToResults() {
	const component = "Orchestrator-Feedback"
	defer o.listenerWg.Done()

	for res := range o.resultChan {
		key := o.pipeline.StatusKey(res.envelope.job)

		if res.err != nil {
			if res.envelope.attempt < o.retryLimit && !o.pipeline.ShouldSkip(res.err, res.envelope.job) {
				res.envelope.attempt++
				if ok := o.enqueue(res.envelope); ok {
					o.appLogger.Warn(component, "Job failed, queuing for retry: key=%s attempt=%d err=%v", key, res.envelope.attempt, res.err)
				} else {
					o.appLogger.Error(component, "Job failed but channel closed, dropping retry: key=%s err=%v", key, res.err)
				}
			} else if o.pipeline.ShouldSkip(res.err, res.envelope.job) {
				o.appLogger.Info(component, "Job marked as skipped: key=%s err=%v", key, res.err)
			} else {
				o.appLogger.Error(component, "Job failed after max retries: key=%s err=%v", key, res.err)
			}
		} else {
			o.appLogger.Info(component, "Job completed successfully: key=%s", key)
			o.mu.Lock()
			o.statusMap[key] = model.IngestionHistory{
				Status:      statusSuccess,
				ProcessedAt: time.Now(),
			}
			o.mu.Unlock()
		}
	}
}

func (o *Orchestrator[J]) enqueue(envelope jobEnvelope[J]) bool {
	o.mu.RLock()
	closed := o.jobChanClosed
	o.mu.RUnlock()
	if closed {
		return false
	}
	o.jobChan <- envelope
	return true
}
