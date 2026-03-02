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

var PortalTransparenciaURL = "https://portaldatransparencia.gov.br/download-de-dados/despesas/"

func NewPortalClient(logger *logger.Logger) service.TransparencyPortalClient {
	return &transparencyPortalClient{
		logger:  logger,
		baseUrl: PortalTransparenciaURL,
		client:  &http.Client{},
	}

}

func (c *transparencyPortalClient) FetchExpensesData(downloadUrl string, date string) service.DownloadResult {
	const component = "Downloader"
	output_path := "tmp/zips/despesas_" + date + ".zip"

	c.logger.Debug(component, "Starting download for date=%s url=%s", date, downloadUrl)

	c.client.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		req.Header.Add("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3")
		return nil
	}
	// Create a new request with a custom User-Agent header
	req, err := http.NewRequest(http.MethodGet, downloadUrl, nil)
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

func (c *transparencyPortalClient) ExtractExpenses(extraction service.OutputExtractionFiles, codes []string, isManagingCode bool) (*service.ExpensesPayload, error) {
	const component = "DataExtractor"
	var wg sync.WaitGroup

	extractionDate, err := time.Parse("20060102", extraction.Date)
	if err != nil {
		return nil, fmt.Errorf("invalid extraction date format: %v", err)
	}
	formattedDate := extractionDate.Format("2006-01-02")

	c.logger.Info(component, "Starting data extraction: date=%s codesCount=%d", formattedDate, len(codes))

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
	if isManagingCode {
		FilterExtractionByColumn(extraction, hasUgCodeAsColumn, codes, "Código Gestão", ugMatches, &wg, c.logger)
	} else {
		FilterExtractionByColumn(extraction, hasUgCodeAsColumn, codes, "Código Unidade Gestora", ugMatches, &wg, c.logger)
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
		if p, ok := extraction.Files[service.DespesasLiquidacaoEmpenhosImpactados]; ok {
			df, err := filesystem.OpenFileAndDecode(p)
			if err != nil {
				return nil, err
			}
			matchedDf := FindRowsSync(df, service.DespesasLiquidacaoEmpenhosImpactados, ugsLiquidations, "Código Liquidação")
			for i := 0; i < matchedDf.Nrow(); i++ {
				liImpacts = append(liImpacts, DfRowToLiquidationImpactedCommitment(matchedDf, i))
			}
		}
	}

	// Extract impacted commitments for payments
	var paImpacts []model.PaymentImpactedCommitment
	if pagamentosDf.Nrow() > 0 {
		ugsPayments := pagamentosDf.Col("Código Pagamento").Records()
		if p, ok := extraction.Files[service.DespesasPagamentoEmpenhosImpactados]; ok {
			df, err := filesystem.OpenFileAndDecode(p)
			if err != nil {
				return nil, err
			}
			matchedDf := FindRowsSync(df, service.DespesasPagamentoEmpenhosImpactados, ugsPayments, "Código Pagamento")
			for i := 0; i < matchedDf.Nrow(); i++ {
				paImpacts = append(paImpacts, DfRowToPaymentImpactedCommitment(matchedDf, i))
			}
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
		FilterExtractionByColumn(extraction, hasCommitmentCodeAsColumn,
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
					items = append(items, DfRowToCommitmentItem(transformedDf, i))
				}
			case service.DespesasItemEmpenhoHistorico:
				for i := 0; i < transformedDf.Nrow(); i++ {
					history = append(history, DfRowToCommitmentItemHistory(transformedDf, i))
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
		commitments = append(commitments, DfRowToCommitment(empenhosDf, i))
	}

	var liquidations []model.Liquidation
	for i := 0; i < liquidacoesDf.Nrow(); i++ {
		liquidations = append(liquidations, DfRowToLiquidation(liquidacoesDf, i))
	}

	var payments []model.Payment
	for i := 0; i < pagamentosDf.Nrow(); i++ {
		payments = append(payments, DfRowToPayment(pagamentosDf, i))
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
