package query

import (
	"fmt"
	"strings"
	"sync"

	"github.com/farxc/transparency_wrapper/internal/logger"
	"github.com/farxc/transparency_wrapper/internal/transparency/converter"
	"github.com/farxc/transparency_wrapper/internal/transparency/files"
	"github.com/farxc/transparency_wrapper/internal/transparency/types"
	"github.com/go-gota/gota/dataframe"
	"github.com/go-gota/gota/series"
)

var columnsForDataType = map[types.DataType][]string{
	types.DespesasPagamento: {
		"Código Pagamento",
		"Código Pagamento Resumido",
		"Data Emissão",
		"Código Tipo Documento",
		"Tipo Documento",
		"Tipo OB",
		"Extraorçamentário",
		"Processo",
		"Código Unidade Gestora",
		"Unidade Gestora",
		"Código Gestão",
		"Gestão",
		"Código Favorecido",
		"Favorecido",
		"Valor Original do Pagamento",
		"Valor do Pagamento Convertido pra R$",
		"Valor Utilizado na Conversão",
	},
	types.DespesasPagamentoEmpenhosImpactados: {
		"Código Pagamento",
		"Código Empenho",
		"Código Natureza Despesa Completa",
		"Subitem",
		"Valor Pago (R$)",
		"Valor Restos a Pagar Inscritos (R$)",
		"Valor Restos a Pagar Cancelado (R$)",
		"Valor Restos a Pagar Pagos (R$)",
	},
	types.DespesasLiquidacao: {
		"Código Liquidação",
		"Código Liquidação Resumido",
		"Data Emissão",
		"Código Tipo Documento",
		"Tipo Documento",
		"Código Unidade Gestora",
		"Unidade Gestora",
		"Código Gestão",
		"Gestão",
		"Código Favorecido",
		"Favorecido",
		"Observação",
	},
	types.DespesasLiquidacaoEmpenhosImpactados: {
		"Código Liquidação",
		"Código Empenho",
		"Código Natureza Despesa Completa",
		"Subitem",
		"Valor Liquidado (R$)",
		"Valor Restos a Pagar Inscritos (R$)",
		"Valor Restos a Pagar Cancelado (R$)",
		"Valor Restos a Pagar Pagos (R$)",
	},
	types.DespesasEmpenho: {
		"Id Empenho",
		"Código Empenho",
		"Código Empenho Resumido",
		"Código Tipo Documento",
		"Tipo Documento",
		"Data Emissão",
		"Tipo Empenho",
		"Código Unidade Gestora",
		"Unidade Gestora",
		"Código Gestão",
		"Gestão",
		"Processo",
		"Favorecido",
		"Código Favorecido",
		"Elemento de Despesa",
		"Plano Orçamentário",
		"Valor Original do Empenho",
		"Valor do Empenho Convertido pra R$",
		"Valor Utilizado na Conversão",
	},
	types.DespesasItemEmpenho: {
		"Id Empenho",
		"Código Empenho",
		"Categoria de Despesa",
		"Grupo de Despesa",
		"Modalidade de Aplicação",
		"Elemento de Despesa",
		"Descrição",
		"Quantidade",
		"Valor Unitário",
		"Valor Total",
		"Sequencial",
		"Valor Atual",
	},
	types.DespesasItemEmpenhoHistorico: {
		"Id Empenho",
		"Código Empenho",
		"Sequencial",
		"Tipo Operação",
		"Data Operação",
		"Quantidade Item",
		"Valor Unitário Item",
		"Valor Total Item",
	},
}

func concatCompleteExpenseNature(originalDf dataframe.DataFrame) (dataframe.DataFrame, error) {
	var expenseNatureCols = []string{"Código Categoria de Despesa", "Código Grupo de Despesa", "Código Modalidade de Aplicação", "Código Elemento de Despesa"}

	completeExpenseNature := originalDf.Select(expenseNatureCols).Rapply(func(s series.Series) series.Series {
		rowValues := s.Records()
		joined := strings.Join(rowValues, ".")
		return series.Strings(joined)
	})
	if completeExpenseNature.Error() != nil {
		return dataframe.DataFrame{}, fmt.Errorf("error creating complete expense nature: %v", completeExpenseNature.Error())
	}

	return originalDf.Mutate(series.New(completeExpenseNature.Col("X0"), series.String, "Natureza de Despesa Completa")), nil
}

func dataframeContainsExpenseNatureColumns(df dataframe.DataFrame) bool {
	var expenseNatureCols = []string{"Código Categoria de Despesa", "Código Grupo de Despesa", "Código Modalidade de Aplicação", "Código Elemento de Despesa"}

	for _, name := range df.Names() {
		for _, col := range expenseNatureCols {
			if name == col {
				return true
			}
		}
	}

	return false
}

// Validates if the data type is supported for transformation
func validateDataTypeForTransformation(dfType types.DataType) error {
	if _, ok := columnsForDataType[dfType]; !ok {
		return fmt.Errorf("unsupported data type for transformation: %v", dfType)
	}
	return nil
}

// Prepares the dataframe by adding computed columns if needed
func prepareDataframeWithComputedColumns(df dataframe.DataFrame) (dataframe.DataFrame, []string, error) {
	result := df
	additionalCols := []string{}

	if dataframeContainsExpenseNatureColumns(df) {
		var err error
		result, err = concatCompleteExpenseNature(df)
		if err != nil {
			return dataframe.DataFrame{}, nil, fmt.Errorf("error concatenating complete expense nature: %v", err)
		}
		additionalCols = append(additionalCols, "Natureza de Despesa Completa")
	}

	return result, additionalCols, nil
}

func SelectDataframeColumns(df dataframe.DataFrame, dfType types.DataType) (dataframe.DataFrame, error) {

	// 1. Validate data type
	if err := validateDataTypeForTransformation(dfType); err != nil {
		return dataframe.DataFrame{}, err
	}

	// 2. Prepare dataframe with computed columns
	result, additionalCols, err := prepareDataframeWithComputedColumns(df)
	if err != nil {
		return dataframe.DataFrame{}, err
	}

	// 3. Select required columns
	selectedCols := append(columnsForDataType[dfType], additionalCols...)
	result = result.Select(selectedCols)

	if result.Error() != nil {
		return dataframe.DataFrame{}, fmt.Errorf("error selecting columns: %v", result.Error())
	}

	return result, nil
}

/*
Finds rows in the dataframe that match the given codes in the specified column and sends the resulting dataframe to the provided channel.
(This function runs as a goroutine and signals completion via the WaitGroup.)
*/
func FindRows(df dataframe.DataFrame, dfType types.DataType, codes []string, codeColumn string, date string, ch chan types.MatchingDataframe, wg *sync.WaitGroup, appLogger *logger.Logger) {
	const component = "DataFilter"
	defer wg.Done()

	appLogger.Debug(component, "Starting row search: date=%s type=%s column=%s codesCount=%d", date, types.DataTypeNames[dfType], codeColumn, len(codes))

	filter := dataframe.F{
		Colname:    codeColumn,
		Comparator: series.In,
		Comparando: codes,
	}

	matchingRows := df.Filter(
		filter,
	)

	if df.Error() != nil {
		appLogger.Error(component, "DataFrame filter error: date=%s type=%s error=%v", date, types.DataTypeNames[dfType], df.Error())
		return
	}

	appLogger.Info(component, "Row search completed: date=%s type=%s matchingRows=%d", date, types.DataTypeNames[dfType], matchingRows.Nrow())
	if matchingRows.Nrow() > 0 {
		ch <- types.MatchingDataframe{Dataframe: matchingRows, Type: dfType}
	}
}

/*
Finds rows in the dataframe that match the given codes in the specified column synchronously.
*/
func FindRowsSync(df dataframe.DataFrame, dfType types.DataType, codes []string, codeColumn string) dataframe.DataFrame {

	filter := dataframe.F{
		Colname:    codeColumn,
		Comparator: series.In,
		Comparando: codes,
	}

	matchingRows := df.Filter(
		filter,
	)

	if df.Error() != nil {
		return dataframe.DataFrame{}
	}
	if matchingRows.Nrow() > 0 {
		matchingDf, err := SelectDataframeColumns(matchingRows, dfType)
		if err != nil {
			return dataframe.DataFrame{}
		}
		return matchingDf
	}

	return dataframe.DataFrame{}
}

func FilterExtractionByColumn(extraction types.OutputExtractionFiles, targetDataTypes []types.DataType, codes []string, matchColumn string, chToRelease chan types.MatchingDataframe, wg *sync.WaitGroup, appLogger *logger.Logger) {
	const component = "ExtractionFilter"
	appLogger.Info(component, "Starting extraction filter: column=%s", matchColumn)
	for _, dt := range targetDataTypes {
		if p, ok := extraction.Files[dt]; ok {
			appLogger.Debug(component, "Processing file: date=%s type=%s path=%s", extraction.Date, types.DataTypeNames[dt], p)
			df, err := files.OpenFileAndDecode(p)
			if err != nil {
				appLogger.Warn(component, "Failed to open/decode file: date=%s type=%s path=%s error=%v", extraction.Date, types.DataTypeNames[dt], p, err)
				continue
			}
			wg.Add(1)
			go FindRows(df, dt, codes, matchColumn, extraction.Date, chToRelease, wg, appLogger)
		}
	}

	appLogger.Info(component, "Extraction filter completed")
}

// attachItemsAndHistoryToCommitment attaches commitment items and their history to a commitment
func AttachItemsAndHistoryToCommitment(
	commitment *types.Commitment,
	itemsDf dataframe.DataFrame,
	historyDf dataframe.DataFrame,
) {
	if itemsDf.Nrow() == 0 {
		return
	}

	for j := 0; j < itemsDf.Nrow(); j++ {
		itemCommitmentCode := itemsDf.Col("Código Empenho").Elem(j).String()
		if itemCommitmentCode != commitment.CommitmentCode {
			continue
		}

		item := converter.DfRowToCommitmentItem(itemsDf, j)

		// Attach history to this item
		if historyDf.Nrow() > 0 {
			itemSequential := itemsDf.Col("Sequencial").Elem(j).String()
			for k := 0; k < historyDf.Nrow(); k++ {
				histCommitmentCode := historyDf.Col("Código Empenho").Elem(k).String()
				histSequential := historyDf.Col("Sequencial").Elem(k).String()
				if histCommitmentCode == commitment.CommitmentCode && histSequential == itemSequential {
					history := converter.DfRowToCommitmentItemHistory(historyDf, k)
					item.History = append(item.History, history)
				}
			}
		}

		commitment.Items = append(commitment.Items, item)
	}
}
