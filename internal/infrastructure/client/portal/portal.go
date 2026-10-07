package portal

import (
	"fmt"
	"net/http"
	"strconv"
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
	debug   bool
	pacer   *pacer
}

var PortalTransparenciaURL = "https://portaldatransparencia.gov.br/download-de-dados/"

func NewTransparencyClient(logger *logger.Logger, debug bool, downloadOpts DownloadOptions) service.TransparencyPortalClient {
	return &transparencyPortalClient{
		logger:  logger,
		baseUrl: PortalTransparenciaURL,
		client:  &http.Client{},
		debug:   debug,
		pacer:   newPacer(downloadOpts.Interval, blockPause),
	}
}

type MatchColumn string

const (
	MatchByManagingCode       MatchColumn = "Código Gestão"
	MatchByManagementUnitCode MatchColumn = "Código Unidade Gestora"
)

func (c *transparencyPortalClient) ExtractExpensesExecution(cfg service.ExpensesExecutionExtractionConfig) (*service.ExpensesExecutionPayload, error) {
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
	filtered_ee := FindRowsSync(ee_df, service.DespesasExecucao, cfg.Codes, string(match_column), c.debug)
	fmt.Printf("Filtered expense execution rows: %d\n", filtered_ee.Nrow())
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
	payload := service.ExpensesExecutionPayload{
		ExtractionDate: cfg.Extraction.Month + "/" + cfg.Extraction.Year,
		UnitsExpenses:  units_expenses_executions,
	}
	return &payload, nil
}

func (c *transparencyPortalClient) FetchExpensesExecution(month, year string) service.DownloadResult {
	url := c.baseUrl + "despesas-execucao/" + year + month
	outputPath := "tmp/zips/expenses_execution/" + year + month + "_Despesas.zip"
	return c.download("month="+month+" year="+year, url, outputPath)
}

func (c *transparencyPortalClient) FetchBudget(year string) service.DownloadResult {
	url := c.baseUrl + "orcamento-despesa/" + year
	outputPath := "tmp/zips/budget/" + year + "_OrcamentoDespesa.zip"
	return c.download("year="+year, url, outputPath)
}

func (c *transparencyPortalClient) ExtractBudget(cfg service.BudgetExtractionConfig) (*service.BudgetPayload, error) {
	const component = "DataExtractor"

	df, err := filesystem.OpenFileAndDecode(cfg.File)
	if err != nil {
		return nil, err
	}

	filtered := FindRowsSync(df, service.OrcamentoDespesa, cfg.Codes, "CÓDIGO ÓRGÃO SUBORDINADO", c.debug)
	c.logger.Info(component, "Filtered budget rows: year=%s rows=%d", cfg.Year, filtered.Nrow())

	if filtered.Nrow() == 0 {
		return nil, fmt.Errorf("dataframe is empty")
	}

	rows := make([]model.ExpenseBudget, 0, filtered.Nrow())
	rowsByAgency := make(map[string]int, len(cfg.Codes))
	for i := 0; i < filtered.Nrow(); i++ {
		row, err := DfRowToExpenseBudget(filtered, i)
		if err != nil {
			return nil, fmt.Errorf("failed to map budget row %d: %w", i, err)
		}
		rows = append(rows, row)
		rowsByAgency[strconv.FormatInt(row.SubordinateAgencyCode, 10)]++
	}
	for _, code := range cfg.Codes {
		if rowsByAgency[code] == 0 {
			c.logger.Warn(component, "No budget rows for subordinate agency code: year=%s code=%s", cfg.Year, code)
		}
	}

	aggregated := service.AggregateBudgetRows(rows)
	if merged := len(rows) - len(aggregated); merged > 0 {
		c.logger.Info(component, "Budget rows sharing a key were summed: year=%s rows=%d merged=%d", cfg.Year, len(rows), merged)
	}

	agencyCodes := make([]int64, 0, len(cfg.Codes))
	for _, code := range cfg.Codes {
		n, err := strconv.ParseInt(code, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid subordinate agency code %q: %w", code, err)
		}
		agencyCodes = append(agencyCodes, n)
	}

	return &service.BudgetPayload{Year: cfg.Year, AgencyCodes: agencyCodes, Rows: aggregated}, nil
}

func (c *transparencyPortalClient) FetchExpensesData(date string) service.DownloadResult {
	url := c.baseUrl + "despesas/" + date
	outputPath := "tmp/zips/expenses/despesas_" + date + ".zip"
	return c.download("date="+date, url, outputPath)
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
			matchedDf := FindRowsSync(df, service.DespesasLiquidacaoEmpenhosImpactados, ugsLiquidations, "Código Liquidação", c.debug)
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
	if pagamentosDf.Nrow() > 0 {
		if p, ok := cfg.Extraction.Files[service.DespesasPagamentoEmpenhosImpactados]; ok {
			df, err := filesystem.OpenFileAndDecode(p)
			if err != nil {
				return nil, err
			}

			matchedDf := matchPaymentImpacts(df, pagamentosDf, c.debug)
			if matchedDf.Error() != nil {
				return nil, fmt.Errorf("failed to filter payment impacted commitments: %w", matchedDf.Error())
			}
			c.logger.Info(component, "Payment impacts matched: date=%s payments=%d impactedRows=%d", extractionDate, pagamentosDf.Nrow(), matchedDf.Nrow())
			if matchedDf.Nrow() == 0 {
				c.logger.Warn(component, "No impacted commitments matched for payments: date=%s payments=%d", extractionDate, pagamentosDf.Nrow())
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

// matchPaymentImpacts keeps the rows of the "EmpenhosImpactados" payment file
// that belong to the given payments. It matches by payment code, not by
// commitment code: a payment usually settles a commitment issued on an earlier
// day (often in an earlier year), so the commitment is not among the day's.
func matchPaymentImpacts(impacts, payments dataframe.DataFrame, debug bool) dataframe.DataFrame {
	if payments.Nrow() == 0 {
		return dataframe.DataFrame{}
	}
	paymentCodes := payments.Col("Código Pagamento").Records()
	return FindRowsSync(impacts, service.DespesasPagamentoEmpenhosImpactados, paymentCodes, "Código Pagamento", debug)
}
