package transparency

import (
	"fmt"
	"sync"
	"time"

	"github.com/farxc/transparency_wrapper/internal/logger"
	"github.com/farxc/transparency_wrapper/internal/transparency/assemble"
	"github.com/farxc/transparency_wrapper/internal/transparency/converter"
	"github.com/farxc/transparency_wrapper/internal/transparency/files"
	"github.com/farxc/transparency_wrapper/internal/transparency/query"
	"github.com/farxc/transparency_wrapper/internal/transparency/types"
	"github.com/go-gota/gota/dataframe"
)

func BuildCommitmentPayload(extraction types.OutputExtractionFiles, unitsCode []string) (*types.CommitmentPayload, error) {

	const component = "CommitmentExtractor"
	extractionDate, err := time.Parse("20060102", extraction.Date)
	if err != nil {
		return nil, fmt.Errorf("invalid extraction date format: %v", err)
	}
	formattedDate := extractionDate.Format("2006-01-02")

	if p, ok := extraction.Files[types.DespesasEmpenho]; ok {
		df, err := files.OpenFileAndDecode(p)
		if err != nil {
			return nil, err
		}

		commitmentsMatchingDf := query.FindRowsSync(df, types.DespesasEmpenho, unitsCode, "Código Unidade Gestora")

		if commitmentsMatchingDf.Nrow() == 0 {
			return nil, fmt.Errorf("no matching commitments found for extraction date %s", extractionDate)
		}
		// Prepare to hold items and history dataframes if needed
		var commitmentsItemsMatchingDf dataframe.DataFrame
		var commitmentItemsHistoryDf dataframe.DataFrame

		// Get commitment codes for sub-extraction
		ugsCommitments := commitmentsMatchingDf.Col("Código Empenho").Records()

		if p, ok := extraction.Files[types.DespesasItemEmpenho]; ok {
			df, err = files.OpenFileAndDecode(p)
			if err != nil {
				return nil, err
			}
			commitmentsItemsMatchingDf = query.FindRowsSync(df, types.DespesasItemEmpenho, ugsCommitments, "Código Empenho")
		}

		if p, ok := extraction.Files[types.DespesasItemEmpenhoHistorico]; ok {
			df, err = files.OpenFileAndDecode(p)
			if err != nil {
				return nil, err
			}
			commitmentItemsHistoryDf = query.FindRowsSync(df, types.DespesasItemEmpenhoHistorico, ugsCommitments, "Código Empenho")
		}

		// Build the hierarchical JSON structure
		payload := &types.CommitmentPayload{
			ExtractionDate:  formattedDate,
			UnitCommitments: []types.UnitCommitments{},
		}

		// Group by Unit Code
		unitMap := make(map[string]*types.UnitCommitments)

		// Helper to get or create unit entry
		getOrCreateUnit := func(ugCode, ugName string) *types.UnitCommitments {
			if _, exists := unitMap[ugCode]; !exists {
				unitMap[ugCode] = &types.UnitCommitments{
					UgCode:       ugCode,
					UgName:       ugName,
					Commitments:  []types.Commitment{},
					Liquidations: []types.Liquidation{},
					Payments:     []types.Payment{},
				}
			}
			// Update name if it was empty
			if unitMap[ugCode].UgName == "" && ugName != "" {
				unitMap[ugCode].UgName = ugName
			}
			return unitMap[ugCode]
		}

		// Process commitments (empenhos)
		for i := 0; i < commitmentsMatchingDf.Nrow(); i++ {
			ugCode := commitmentsMatchingDf.Col("Código Unidade Gestora").Elem(i).String()
			ugName := commitmentsMatchingDf.Col("Unidade Gestora").Elem(i).String()
			unit := getOrCreateUnit(ugCode, ugName)

			commitment := converter.DfRowToCommitment(commitmentsMatchingDf, i)

			query.AttachItemsAndHistoryToCommitment(&commitment, commitmentsItemsMatchingDf, commitmentItemsHistoryDf)

			unit.Commitments = append(unit.Commitments, commitment)
		}

		// Convert map to slice
		for _, unit := range unitMap {
			payload.UnitCommitments = append(payload.UnitCommitments, *unit)
		}

		return payload, nil

	}
	return nil, fmt.Errorf("no commitment file found in extraction for date %s", extractionDate)
}

// To do: Modularize this function further
func ExtractData(extraction types.OutputExtractionFiles, unitsCode []string, appLogger *logger.Logger) (*types.CommitmentPayload, error) {
	const component = "DataExtractor"
	var wg sync.WaitGroup

	extractionDate, err := time.Parse("20060102", extraction.Date)
	if err != nil {
		return nil, fmt.Errorf("invalid extraction date format: %v", err)
	}
	formattedDate := extractionDate.Format("2006-01-02")

	appLogger.Info(component, "Starting data extraction: date=%s unitsCount=%d", formattedDate, len(unitsCode))

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
	query.FilterExtractionByColumn(extraction, hasUgCodeAsColumn, unitsCode, "Código Unidade Gestora", ugMatches, &wg, appLogger)

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
