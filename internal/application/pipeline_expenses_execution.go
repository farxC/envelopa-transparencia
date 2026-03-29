package application

import (
	"context"
	"fmt"
	"path/filepath"
	"strconv"
	"time"

	"github.com/farxc/envelopa-transparencia/internal/domain/model"
	"github.com/farxc/envelopa-transparencia/internal/domain/service"
	"github.com/farxc/envelopa-transparencia/internal/infrastructure/filesystem"
	"github.com/farxc/envelopa-transparencia/internal/infrastructure/logger"
	"github.com/farxc/envelopa-transparencia/internal/infrastructure/store"
	"github.com/lib/pq"
)

// ExpensesExecutionPipeline implements Pipeline[model.ExpensesExecutionJob].
// It handles monthly aggregated execution data from the despesas-execucao endpoint.
type ExpensesExecutionPipeline struct {
	client    service.TransparencyPortalClient
	loader    service.Loader
	appLogger *logger.Logger
}

func NewExpensesExecutionPipeline(
	client service.TransparencyPortalClient,
	loader service.Loader,
	appLogger *logger.Logger,
) *ExpensesExecutionPipeline {
	return &ExpensesExecutionPipeline{
		client:    client,
		loader:    loader,
		appLogger: appLogger,
	}
}

func (p *ExpensesExecutionPipeline) Execute(ctx context.Context, job model.ExpensesExecutionJob) error {
	// 1. Download
	download := p.client.FetchExpensesExecution(job.Month, job.Year)
	if !download.Success {
		return fmt.Errorf("download failed for %s-%s", job.Year, job.Month)
	}

	// 2. Unzip
	outputDir := "tmp/data/expenses_execution_" + job.Year + job.Month
	extraction := filesystem.UnzipFile(download.OutputPath, outputDir, p.appLogger)
	if !extraction.Success {
		return fmt.Errorf("extraction failed for %s-%s", job.Year, job.Month)
	}

	// 3. Build extraction config
	codeStrings := make([]string, len(job.Codes))
	for i, c := range job.Codes {
		codeStrings[i] = fmt.Sprintf("%d", c)
	}

	csvFile := filepath.Join(extraction.OutputDir, job.Year+job.Month+service.DespesasExecucaoDataType)

	cfg := service.ExpensesExecutionExtractionConfig{
		Codes:          codeStrings,
		IsManagingCode: job.IsManagingCode,
		Extraction: service.OutputExpensesExecutionExtractionFiles{
			Month: job.Month,
			Year:  job.Year,
			File:  csvFile,
		},
	}

	// 3. Extract
	payload, err := p.client.ExtractExpensesExecution(cfg)
	if err != nil {
		return err
	}

	// 4. Load (TODO: implement store for execution data)
	err = p.loader.LoadExpensesExecution(ctx, payload)
	if err != nil {
		return err
	}
	return nil
}

func (p *ExpensesExecutionPipeline) BuildHistoryRecord(job model.ExpensesExecutionJob) *model.IngestionHistory {
	scope := store.ScopeTypeManagingUnit
	if job.IsManagingCode {
		scope = store.ScopeTypeManagement
	}

	year, _ := strconv.Atoi(job.Year)
	month, _ := strconv.Atoi(job.Month)
	refDate := time.Date(year, time.Month(month), 1, 0, 0, 0, 0, time.UTC)

	return &model.IngestionHistory{
		ReferenceDate:  refDate,
		TriggerType:    job.Trigger,
		ScopeType:      scope,
		SourceFile:     fmt.Sprintf("%s%s_despesas.zip", job.Year, job.Month),
		ProcessedCodes: pq.Int64Array(job.Codes),
	}
}

func (p *ExpensesExecutionPipeline) ShouldSkip(err error, job model.ExpensesExecutionJob) bool {
	return err.Error() == "dataframe is empty"
}

func (p *ExpensesExecutionPipeline) StatusKey(job model.ExpensesExecutionJob) string {
	return job.Year + "-" + job.Month
}

func (p *ExpensesExecutionPipeline) HistoryKey(h model.IngestionHistory) string {
	return h.ReferenceDate.Format("2006-01")
}

func (p *ExpensesExecutionPipeline) HistoryRange(startDate, endDate time.Time) (time.Time, time.Time) {
	start := time.Date(startDate.Year(), startDate.Month(), 1, 0, 0, 0, 0, startDate.Location())
	end := time.Date(endDate.Year(), endDate.Month(), 1, 0, 0, 0, 0, endDate.Location())
	return start, end
}
