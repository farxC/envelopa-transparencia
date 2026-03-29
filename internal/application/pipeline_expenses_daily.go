package application

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/farxc/envelopa-transparencia/internal/domain/model"
	"github.com/farxc/envelopa-transparencia/internal/domain/service"
	"github.com/farxc/envelopa-transparencia/internal/infrastructure/filesystem"
	"github.com/farxc/envelopa-transparencia/internal/infrastructure/logger"
	"github.com/farxc/envelopa-transparencia/internal/infrastructure/store"
	"github.com/lib/pq"
)

// ExpensesDailyPipeline implements Pipeline[model.ExpensesDailyJob].
// It handles daily granularity lifecycle data: empenho → liquidação → pagamento.
type ExpensesDailyPipeline struct {
	client    service.TransparencyPortalClient
	loader    service.Loader
	appLogger *logger.Logger
}

func NewExpensesDailyPipeline(
	client service.TransparencyPortalClient,
	loader service.Loader,
	appLogger *logger.Logger,
) *ExpensesDailyPipeline {
	return &ExpensesDailyPipeline{
		client:    client,
		loader:    loader,
		appLogger: appLogger,
	}
}

func (p *ExpensesDailyPipeline) Execute(ctx context.Context, job model.ExpensesDailyJob) error {
	dateCode := job.Date.Format("20060102")

	// 1. Download if not already present
	zipPath := "tmp/zips/expenses/despesas_" + dateCode + ".zip"
	if _, err := os.Stat(zipPath); os.IsNotExist(err) {
		download := p.client.FetchExpensesData(dateCode)
		if !download.Success {
			return fmt.Errorf("download failed")
		}
	}

	// 2. Unzip
	outputDir := "tmp/data/despesas_" + dateCode
	extraction := filesystem.UnzipFile(zipPath, outputDir, p.appLogger)
	if !extraction.Success {
		return fmt.Errorf("extraction failed")
	}

	// 3. Build extraction config
	codeStrings := make([]string, len(job.Codes))
	for i, c := range job.Codes {
		codeStrings[i] = fmt.Sprintf("%d", c)
	}

	cfg := service.ExpensesExtractionConfig{
		Codes:          codeStrings,
		IsManagingCode: job.IsManagingCode,
		Extraction: service.OutputExpensesExtractionFiles{
			Date:  dateCode,
			Files: filesystem.BuildFilesForDate(dateCode, extraction.OutputDir),
		},
	}

	// 4. Extract
	payload, err := p.client.ExtractExpenses(cfg)
	if err != nil {
		return err
	}

	// 5. Load
	return p.loader.LoadExpenses(ctx, payload)
}

func (p *ExpensesDailyPipeline) BuildHistoryRecord(job model.ExpensesDailyJob) *model.IngestionHistory {
	scope := store.ScopeTypeManagingUnit
	if job.IsManagingCode {
		scope = store.ScopeTypeManagement
	}
	return &model.IngestionHistory{
		ReferenceDate:  job.Date,
		TriggerType:    job.Trigger,
		ScopeType:      scope,
		SourceFile:     fmt.Sprintf("despesas_%s.zip", job.Date.Format("20060102")),
		ProcessedCodes: pq.Int64Array(job.Codes),
	}
}

func (p *ExpensesDailyPipeline) ShouldSkip(err error, job model.ExpensesDailyJob) bool {
	return err.Error() == fmt.Sprintf("no matching data found for extraction date %s", job.Date.Format("2006-01-02")) ||
		err.Error() == "dataframe is empty"
}

func (p *ExpensesDailyPipeline) StatusKey(job model.ExpensesDailyJob) string {
	return job.Date.Format(time.DateOnly)
}

func (p *ExpensesDailyPipeline) HistoryKey(h model.IngestionHistory) string {
	return h.ReferenceDate.Format(time.DateOnly)
}

func (p *ExpensesDailyPipeline) HistoryRange(startDate, endDate time.Time) (time.Time, time.Time) {
	return startDate, endDate
}
