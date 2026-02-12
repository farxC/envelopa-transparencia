package transparency

import (
	"fmt"
	"sync"
	"time"

	"github.com/farxc/envelopa-transparencia/internal/logger"
	"github.com/farxc/envelopa-transparencia/internal/transparency/assemble"
	"github.com/farxc/envelopa-transparencia/internal/transparency/files"
	"github.com/farxc/envelopa-transparencia/internal/transparency/query"
	"github.com/farxc/envelopa-transparencia/internal/transparency/types"
	"github.com/go-gota/gota/dataframe"
)

// To do: Modularize this function further
func ExtractData(extraction types.OutputExtractionFiles, codes []string, isManagingCode bool, appLogger *logger.Logger) (*types.CommitmentPayload, error) {
	const component = "DataExtractor"
	var wg sync.WaitGroup

	extractionDate, err := time.Parse("20060102", extraction.Date)
	if err != nil {
		return nil, fmt.Errorf("invalid extraction date format: %v", err)
	}
	formattedDate := extractionDate.Format("2006-01-02")

	appLogger.Info(component, "Starting data extraction: date=%s codesCount=%d", formattedDate, len(codes))

	// Channel for collect DataFrames based in Unit Codes
	ugMatches := make(chan types.MatchingDataframe, 3)

	// Channel for collect DataFrames based in Commitment Codes
	commitmentMatches := make(chan types.MatchingDataframe, 3)

	hasUgCodeAsColumn := []types.DataType{
		types.DespesasEmpenho,
		types.DespesasLiquidacao,
		types.DespesasPagamento,
	}

	hasCommitmentCodeAsColumn := []types.DataType{
		types.DespesasItemEmpenho,
		types.DespesasItemEmpenhoHistorico,
	}

	appLogger.Debug(component, "Phase 1: Filtering by UG codes: date=%s", extractionDate)
	// First, find all Commitments based in Unit Codes
	if isManagingCode {
		query.FilterExtractionByColumn(extraction, hasUgCodeAsColumn, codes, "Código Gestão", ugMatches, &wg, appLogger)
	} else {
		query.FilterExtractionByColumn(extraction, hasUgCodeAsColumn, codes, "Código Unidade Gestora", ugMatches, &wg, appLogger)
	}

	wg.Wait()
	close(ugMatches)

	// Collect DataFrames by type
	empenhosDf := dataframe.New()
	liquidacoesDf := dataframe.New()
	pagamentosDf := dataframe.New()
	for extracted := range ugMatches {
		transformedDf, err := query.SelectDataframeColumns(extracted.Dataframe, extracted.Type)

		if err != nil {
			appLogger.Error(component, "DataFrame transformation error: date=%s type=%s error=%v", extractionDate, types.DataTypeNames[extracted.Type], err)
			continue
		}

		appLogger.Debug(component, "DataFrame transformed: date=%s type=%s rows=%d", extractionDate, types.DataTypeNames[extracted.Type], transformedDf.Nrow())

		switch extracted.Type {
		case types.DespesasEmpenho:
			empenhosDf = transformedDf
		case types.DespesasLiquidacao:
			liquidacoesDf = transformedDf
		case types.DespesasPagamento:
			pagamentosDf = transformedDf
		}
	}

	// Check if we have ANY data at all
	hasAnyData := empenhosDf.Nrow() > 0 || liquidacoesDf.Nrow() > 0 || pagamentosDf.Nrow() > 0
	appLogger.Info(component, "Phase 1 completed: date=%s empenhos=%d liquidacoes=%d pagamentos=%d", extractionDate, empenhosDf.Nrow(), liquidacoesDf.Nrow(), pagamentosDf.Nrow())

	if !hasAnyData {
		appLogger.Warn(component, "No matching data found: date=%s", extractionDate)
		return nil, fmt.Errorf("no matching data found for extraction date %s", extractionDate)
	}

	// Extract impacted commitments for liquidations
	liquidationImpactedCommitmentsDf := dataframe.New()
	if liquidacoesDf.Nrow() > 0 {
		ugsLiquidations := liquidacoesDf.Col("Código Liquidação").Records()
		if p, ok := extraction.Files[types.DespesasLiquidacaoEmpenhosImpactados]; ok {
			df, err := files.OpenFileAndDecode(p)
			if err != nil {
				return nil, err
			}
			liquidationImpactedCommitmentsDf = query.FindRowsSync(df, types.DespesasLiquidacaoEmpenhosImpactados, ugsLiquidations, "Código Liquidação")
		}
	}

	// Extract impacted commitments for payments
	paymentImpactedCommitmentsDf := dataframe.New()
	if pagamentosDf.Nrow() > 0 {
		ugsPayments := pagamentosDf.Col("Código Pagamento").Records()
		if p, ok := extraction.Files[types.DespesasPagamentoEmpenhosImpactados]; ok {
			df, err := files.OpenFileAndDecode(p)
			if err != nil {
				return nil, err
			}
			paymentImpactedCommitmentsDf = query.FindRowsSync(df, types.DespesasPagamentoEmpenhosImpactados, ugsPayments, "Código Pagamento")
		}
	}

	// Only extract commitment items if we have commitments
	itemsDf := dataframe.DataFrame{}
	historyDf := dataframe.DataFrame{}

	if empenhosDf.Nrow() > 0 {
		// Get commitment codes for sub-extraction
		ugsCommitments := empenhosDf.Col("Código Empenho").Records()
		appLogger.Debug(component, "Phase 2: Extracting commitment items: date=%s commitmentCodes=%d", extractionDate, len(ugsCommitments))

		// Extract commitment items and history
		query.FilterExtractionByColumn(extraction, hasCommitmentCodeAsColumn,
			ugsCommitments, "Código Empenho", commitmentMatches, &wg, appLogger)
		wg.Wait()
		close(commitmentMatches)

		for extracted := range commitmentMatches {
			transformedDf, err := query.SelectDataframeColumns(extracted.Dataframe, extracted.Type)
			if err != nil {
				appLogger.Error(component, "Commitment items transformation error: date=%s type=%s error=%v", extractionDate, types.DataTypeNames[extracted.Type], err)
				continue
			}

			switch extracted.Type {
			case types.DespesasItemEmpenho:
				itemsDf = transformedDf
			case types.DespesasItemEmpenhoHistorico:
				historyDf = transformedDf
			}
		}
		appLogger.Info(component, "Phase 2 completed: date=%s items=%d history=%d", extractionDate, itemsDf.Nrow(), historyDf.Nrow())
	} else {
		appLogger.Debug(component, "Skipping Phase 2 (no commitments): date=%s", extractionDate)
		// Close the channel since we won't use it
		close(commitmentMatches)
	}

	// Build the hierarchical JSON structure
	payload := &types.CommitmentPayload{
		ExtractionDate:  formattedDate,
		UnitCommitments: []types.UnitCommitments{},
	}

	unitsMap := assemble.AssembleExpensesData(assemble.ExpectedDataFrames{
		CommitmentsDf:                    empenhosDf,
		CommitmentItemsDf:                itemsDf,
		CommitmentHistoryDf:              historyDf,
		LiquidationDf:                    liquidacoesDf,
		LiquidationImpactedCommitmentsDf: liquidationImpactedCommitmentsDf,
		PaymentsDf:                       pagamentosDf,
		PaymentImpactedCommitmentsDf:     paymentImpactedCommitmentsDf,
	})

	// Convert map to slice
	for _, unit := range unitsMap {
		payload.UnitCommitments = append(payload.UnitCommitments, *unit)
	}

	appLogger.Info(component, "Extraction completed: date=%s unitsProcessed=%d", extractionDate, len(payload.UnitCommitments))
	return payload, nil
}
