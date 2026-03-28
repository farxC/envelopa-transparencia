package portal

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/farxc/envelopa-transparencia/internal/domain/model"
	"github.com/farxc/envelopa-transparencia/internal/domain/service"
	"github.com/farxc/envelopa-transparencia/internal/infrastructure/filesystem"
	"github.com/farxc/envelopa-transparencia/internal/infrastructure/logger"
	"github.com/go-gota/gota/dataframe"
)

type transparencyPortalClient struct {
	logger  *logger.Logger
	baseUrl string
	client  *http.Client
}

var PortalTransparenciaURL = "https://portaldatransparencia.gov.br/download-de-dados/"

func NewTransparencyClient(logger *logger.Logger) service.TransparencyPortalClient {
	return &transparencyPortalClient{
		logger:  logger,
		baseUrl: PortalTransparenciaURL,
		client:  &http.Client{},
	}

}

type MatchColumn string

const (
	MatchByManagingCode       MatchColumn = "Código Gestão"
	MatchByManagementUnitCode MatchColumn = "Código Unidade Gestora"
)

func (c *transparencyPortalClient) ExtractExpensesExecution(cfg service.ExpensesExecutionExtractionConfig) (*[]service.UnitExpenseExecution, error) {
	var units_expenses_executions []service.UnitExpenseExecution
	ee_df, err := filesystem.OpenFileAndDecode(cfg.Extraction.File)
	if err != nil {
		return nil, err
	}
	var match_column MatchColumn
	if cfg.IsManagingCode {
		match_column = MatchByManagingCode
	} else {
		match_column = MatchByManagementUnitCode
	}
	filtered_ee := FindRowsSync(ee_df, service.DespesasExecucao, cfg.Codes, string(match_column))

	for i := 0; i < filtered_ee.Nrow(); i++ {
		expense_execution, err := DfRowToExpenseExecution(filtered_ee, i)
		if err != nil {
			return nil, fmt.Errorf("failed to map expense execution row %d: %w", i, err)
		}
		uee := service.UnitExpenseExecution{
			UgCode:           expense_execution.ManagementUnitCode,
			UgName:           expense_execution.ManagementUnitName,
			ExpenseExecution: expense_execution,
		}
		units_expenses_executions = append(units_expenses_executions, uee)
	}

	return &units_expenses_executions, nil
}

func (c *transparencyPortalClient) FetchExpensesExecution(month, year string) service.DownloadResult {
	const component = "Downloader"
	url := c.baseUrl + "despesas-execucao/" + year + month
	output_path := "tmp/zips/execution/" + year + month + "_despesas.zip"

	c.logger.Debug(component, "Starting download for month=%s year=%s url=%s", month, year, url)

	c.client.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		req.Header.Add("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3")
		return nil
	}
	// Create a new request with a custom User-Agent header
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		c.logger.Error(component, "Failed to create HTTP request: month=%s year=%s error=%v", month, year, err)
		return service.DownloadResult{Success: false}
	}

	resp, err := c.client.Do(req)

	if err != nil {
		c.logger.Error(component, "HTTP request failed: month=%s year=%s error=%v", month, year, err)
		return service.DownloadResult{Success: false}
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		c.logger.Warn(component, "Non-OK HTTP response: month=%s year=%s status=%s statusCode=%d", month, year, resp.Status, resp.StatusCode)
		return service.DownloadResult{Success: false}
	}

	out, err := os.Create(output_path)

	if err != nil {
		c.logger.Error(component, "Failed to create output file: month=%s year=%s path=%s error=%v", month, year, output_path, err)
		return service.DownloadResult{Success: false}
	}
	defer out.Close()

	bytesWritten, err := io.Copy(out, resp.Body)
	if err != nil {
		c.logger.Error(component, "Failed to write data to file: month=%s year=%s error=%v", month, year, err)
		return service.DownloadResult{Success: false}
	}

	c.logger.Info(component, "Download completed: month=%s year=%s path=%s size=%d bytes", month, year, output_path, bytesWritten)
	return service.DownloadResult{Success: true, OutputPath: output_path}
}

func (c *transparencyPortalClient) FetchExpensesData(date string) service.DownloadResult {
	component := "Downloader"
	url := c.baseUrl + "despesas/" + date
	output_path := "tmp/zips/despesas_" + date + ".zip"

	c.logger.Debug(component, "Starting download for date=%s url=%s", date, url)

	c.client.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		req.Header.Add("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3")
		return nil
	}
	// Create a new request with a custom User-Agent header
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		c.logger.Error(component, "Failed to create HTTP request: date=%s error=%v", date, err)
		return service.DownloadResult{Success: false}
	}

	resp, err := c.client.Do(req)

	if err != nil {
		c.logger.Error(component, "HTTP request failed: date=%s error=%v", date, err)
		return service.DownloadResult{Success: false}
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		c.logger.Warn(component, "Non-OK HTTP response: date=%s status=%s statusCode=%d", date, resp.Status, resp.StatusCode)
		return service.DownloadResult{Success: false}
	}

	out, err := os.Create(output_path)

	if err != nil {
		c.logger.Error(component, "Failed to create output file: date=%s path=%s error=%v", date, output_path, err)
		return service.DownloadResult{Success: false}
	}
	defer out.Close()

	bytesWritten, err := io.Copy(out, resp.Body)
	if err != nil {
		c.logger.Error(component, "Failed to write data to file: date=%s error=%v", date, err)
		return service.DownloadResult{Success: false}
	}

	c.logger.Info(component, "Download completed: date=%s path=%s size=%d bytes", date, output_path, bytesWritten)
	return service.DownloadResult{Success: true, OutputPath: output_path}
}

func (c *transparencyPortalClient) ExtractExpenses(cfg service.ExpensesExtractionConfig) (*service.ExpensesPayload, error) {
	var wg sync.WaitGroup
	component := "DataExtractor"

	extractionDate, err := time.Parse("20060102", cfg.Extraction.Date)
	if err != nil {
		return nil, fmt.Errorf("invalid extraction date format: %v", err)
	}
	formattedDate := extractionDate.Format("2006-01-02")

	c.logger.Info(component, "Starting data extraction: date=%s codesCount=%d", formattedDate, len(cfg.Codes))

	// Channel for collect DataFrames based in Unit Codes
	ugMatches := make(chan service.MatchingDataframe, 3)

	// Channel for collect DataFrames based in Commitment Codes
	commitmentMatches := make(chan service.MatchingDataframe, 3)

	hasUgCodeAsColumn := []service.DataType{
		service.DespesasEmpenho,
		service.DespesasLiquidacao,
		service.DespesasPagamento,
	}

	hasCommitmentCodeAsColumn := []service.DataType{
		service.DespesasItemEmpenho,
		service.DespesasItemEmpenhoHistorico,
	}

	c.logger.Debug(component, "Phase 1: Filtering by UG codes: date=%s", extractionDate)
	// First, find all Commitments based in Unit Codes
	if cfg.IsManagingCode {
		FilterExtractionByColumn(cfg.Extraction, hasUgCodeAsColumn, cfg.Codes, "Código Gestão", ugMatches, &wg, c.logger)
	} else {
		FilterExtractionByColumn(cfg.Extraction, hasUgCodeAsColumn, cfg.Codes, "Código Unidade Gestora", ugMatches, &wg, c.logger)
	}

	wg.Wait()
	close(ugMatches)

	// Collect DataFrames by type
	empenhosDf := dataframe.New()
	liquidacoesDf := dataframe.New()
	pagamentosDf := dataframe.New()
	for extracted := range ugMatches {
		transformedDf, err := SelectDataframeColumns(extracted.Dataframe, extracted.Type)

		if err != nil {
			c.logger.Error(component, "DataFrame transformation error: date=%s type=%s error=%v", extractionDate, service.DataTypeNames[extracted.Type], err)
			continue
		}

		c.logger.Debug(component, "DataFrame transformed: date=%s type=%s rows=%d", extractionDate, service.DataTypeNames[extracted.Type], transformedDf.Nrow())

		switch extracted.Type {
		case service.DespesasEmpenho:
			empenhosDf = transformedDf
		case service.DespesasLiquidacao:
			liquidacoesDf = transformedDf
		case service.DespesasPagamento:
			pagamentosDf = transformedDf
		}
	}

	// Check if we have ANY data at all
	hasAnyData := empenhosDf.Nrow() > 0 || liquidacoesDf.Nrow() > 0 || pagamentosDf.Nrow() > 0
	c.logger.Info(component, "Phase 1 completed: date=%s empenhos=%d liquidacoes=%d pagamentos=%d", extractionDate, empenhosDf.Nrow(), liquidacoesDf.Nrow(), pagamentosDf.Nrow())

	if !hasAnyData {
		c.logger.Warn(component, "No matching data found: date=%s", extractionDate)
		return nil, fmt.Errorf("no matching data found for extraction date %s", extractionDate.Format("2006-01-02"))
	}

	// Extract impacted commitments for liquidations
	var liImpacts []model.LiquidationImpactedCommitment
	if liquidacoesDf.Nrow() > 0 {
		ugsLiquidations := liquidacoesDf.Col("Código Liquidação").Records()
		if p, ok := cfg.Extraction.Files[service.DespesasLiquidacaoEmpenhosImpactados]; ok {
			df, err := filesystem.OpenFileAndDecode(p)
			if err != nil {
				return nil, err
			}
			matchedDf := FindRowsSync(df, service.DespesasLiquidacaoEmpenhosImpactados, ugsLiquidations, "Código Liquidação")
			for i := 0; i < matchedDf.Nrow(); i++ {
				imp, err := DfRowToLiquidationImpactedCommitment(matchedDf, i)
				if err != nil {
					return nil, fmt.Errorf("failed to map liquidation impacted commitment row %d: %w", i, err)
				}
				liImpacts = append(liImpacts, imp)
			}
		}
	}

	// Extract impacted commitments for payments
	var paImpacts []model.PaymentImpactedCommitment
	if empenhosDf.Nrow() > 0 {
		ugsCommitments := empenhosDf.Col("Código Empenho").Records()
		if p, ok := cfg.Extraction.Files[service.DespesasPagamentoEmpenhosImpactados]; ok {
			df, err := filesystem.OpenFileAndDecode(p)
			if err != nil {
				return nil, err
			}

			matchedDf := FindRowsSync(df, service.DespesasPagamentoEmpenhosImpactados, ugsCommitments, "Código Empenho")
			if matchedDf.Error() != nil {
				return nil, fmt.Errorf("failed to filter payment impacted commitments: %w", matchedDf.Error())
			}
			c.logger.Info(component, "Payment impacts matched: date=%s commitments=%d impactedRows=%d", extractionDate, len(ugsCommitments), matchedDf.Nrow())
			if matchedDf.Nrow() == 0 {
				c.logger.Warn(component, "No impacted commitments matched for payment commitments: date=%s commitments=%d", extractionDate, len(ugsCommitments))
			}
			for i := 0; i < matchedDf.Nrow(); i++ {
				imp, err := DfRowToPaymentImpactedCommitment(matchedDf, i)
				if err != nil {
					return nil, fmt.Errorf("failed to map payment impacted commitment row %d: %w", i, err)
				}
				paImpacts = append(paImpacts, imp)
			}
		} else {
			c.logger.Warn(component, "Payment impacted commitments file not found: date=%s", extractionDate)
		}
	}

	// Only extract commitment items if we have commitments
	var items []model.CommitmentItem
	var history []model.CommitmentItemsHistory

	if empenhosDf.Nrow() > 0 {
		// Get commitment codes for sub-extraction
		ugsCommitments := empenhosDf.Col("Código Empenho").Records()
		c.logger.Debug(component, "Phase 2: Extracting commitment items: date=%s commitmentCodes=%d", extractionDate, len(ugsCommitments))

		// Extract commitment items and history
		FilterExtractionByColumn(cfg.Extraction, hasCommitmentCodeAsColumn,
			ugsCommitments, "Código Empenho", commitmentMatches, &wg, c.logger)
		wg.Wait()
		close(commitmentMatches)

		for extracted := range commitmentMatches {
			transformedDf, err := SelectDataframeColumns(extracted.Dataframe, extracted.Type)
			if err != nil {
				c.logger.Error(component, "Commitment items transformation error: date=%s type=%s error=%v", extractionDate, service.DataTypeNames[extracted.Type], err)
				continue
			}

			switch extracted.Type {
			case service.DespesasItemEmpenho:
				for i := 0; i < transformedDf.Nrow(); i++ {
					item, err := DfRowToCommitmentItem(transformedDf, i)
					if err != nil {
						return nil, fmt.Errorf("failed to map commitment item row %d: %w", i, err)
					}
					items = append(items, item)
				}
			case service.DespesasItemEmpenhoHistorico:
				for i := 0; i < transformedDf.Nrow(); i++ {
					entry, err := DfRowToCommitmentItemHistory(transformedDf, i)
					if err != nil {
						return nil, fmt.Errorf("failed to map commitment item history row %d: %w", i, err)
					}
					history = append(history, entry)
				}
			}
		}
		c.logger.Info(component, "Phase 2 completed: date=%s items=%d history=%d", extractionDate, len(items), len(history))
	} else {
		c.logger.Debug(component, "Skipping Phase 2 (no commitments): date=%s", extractionDate)
		// Close the channel since we won't use it
		close(commitmentMatches)
	}

	// Map raw rows to models
	var commitments []model.Commitment
	for i := 0; i < empenhosDf.Nrow(); i++ {
		commitment, err := DfRowToCommitment(empenhosDf, i)
		if err != nil {
			return nil, fmt.Errorf("failed to map commitment row %d: %w", i, err)
		}
		commitments = append(commitments, commitment)
	}

	var liquidations []model.Liquidation
	for i := 0; i < liquidacoesDf.Nrow(); i++ {
		liquidations = append(liquidations, DfRowToLiquidation(liquidacoesDf, i))
	}

	var payments []model.Payment
	for i := 0; i < pagamentosDf.Nrow(); i++ {
		payment, err := DfRowToPayment(pagamentosDf, i)
		if err != nil {
			return nil, fmt.Errorf("failed to map payment row %d: %w", i, err)
		}
		payments = append(payments, payment)
	}

	// Build the hierarchical structure using the agnostic domain service
	unitsMap := service.AssembleExpensesData(
		commitments,
		items,
		history,
		liquidations,
		liImpacts,
		payments,
		paImpacts,
	)

	// Build the hierarchical JSON structure
	payload := &service.ExpensesPayload{
		ExtractionDate: formattedDate,
		UnitsExpenses:  []service.UnitsExpenses{},
	}

	// Convert map to slice
	for _, unit := range unitsMap {
		payload.UnitsExpenses = append(payload.UnitsExpenses, *unit)
	}

	c.logger.Info(component, "Extraction completed: date=%s unitsProcessed=%d", extractionDate, len(payload.UnitsExpenses))
	return payload, nil
}
