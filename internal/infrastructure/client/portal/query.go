package portal

import (
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/farxc/envelopa-transparencia/internal/domain/service"
	"github.com/farxc/envelopa-transparencia/internal/infrastructure/filesystem"
	"github.com/farxc/envelopa-transparencia/internal/infrastructure/logger"
	"github.com/go-gota/gota/dataframe"
	"github.com/go-gota/gota/series"
)

var columnsForDataType = map[service.DataType][]string{
	service.DespesasPagamento: {
		"Código Pagamento",
		"Código Pagamento Resumido",
		"Data Emissão",
		"Código Tipo Documento",
		"Tipo Documento",
		"Tipo OB",
		"Extraorçamentário",
		"Processo",
		"Código Unidade Gestora",
		"Código Categoria de Despesa",
		"Categoria de Despesa",
		"Código Grupo de Despesa",
		"Grupo de Despesa",
		"Código Modalidade de Aplicação",
		"Modalidade de Aplicação",
		"Código Elemento de Despesa",
		"Elemento de Despesa",
		"Plano Orçamentário",
		"Código Plano Orçamentário",
		"Observação",
		"Unidade Gestora",
		"Código Gestão",
		"Gestão",
		"Código Favorecido",
		"Favorecido",
		"Valor Original do Pagamento",
		"Valor do Pagamento Convertido pra R$",
		"Valor Utilizado na Conversão",
	},
	service.DespesasPagamentoEmpenhosImpactados: {
		"Código Pagamento",
		"Código Empenho",
		"Código Natureza Despesa Completa",
		"Subitem",
		"Valor Pago (R$)",
		"Valor Restos a Pagar Inscritos (R$)",
		"Valor Restos a Pagar Cancelado (R$)",
		"Valor Restos a Pagar Pagos (R$)",
	},
	service.DespesasLiquidacao: {
		"Código Liquidação",
		"Código Liquidação Resumido",
		"Data Emissão",
		"Código Tipo Documento",
		"Tipo Documento",
		"Código Unidade Gestora",
		"Unidade Gestora",
		"Código Gestão",
		"Gestão",
		"Código Categoria de Despesa",
		"Categoria de Despesa",
		"Código Grupo de Despesa",
		"Grupo de Despesa",
		"Código Modalidade de Aplicação",
		"Modalidade de Aplicação",
		"Código Elemento de Despesa",
		"Elemento de Despesa",
		"Plano Orçamentário",
		"Código Plano Orçamentário",
		"Código Favorecido",
		"Favorecido",
		"Observação",
	},
	service.DespesasLiquidacaoEmpenhosImpactados: {
		"Código Liquidação",
		"Código Empenho",
		"Código Natureza Despesa Completa",
		"Subitem",
		"Valor Liquidado (R$)",
		"Valor Restos a Pagar Inscritos (R$)",
		"Valor Restos a Pagar Cancelado (R$)",
		"Valor Restos a Pagar Pagos (R$)",
	},
	service.DespesasEmpenho: {
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
		"Plano Orçamentário",
		"Código Plano Orçamentário",
		"Favorecido",
		"Código Favorecido",
		"Observação",
		"Tipo Crédito",
		"Código Grupo Fonte Recurso",
		"Grupo Fonte Recurso",
		"Código Categoria de Despesa",
		"Categoria de Despesa",
		"Código Grupo de Despesa",
		"Grupo de Despesa",
		"Código Modalidade de Aplicação",
		"Modalidade de Aplicação",
		"Código Elemento de Despesa",
		"Elemento de Despesa",
		"Modalidade de Licitação",
		"Valor Original do Empenho",
		"Valor do Empenho Convertido pra R$",
		"Valor Utilizado na Conversão",
	},
	service.DespesasItemEmpenho: {
		"Id Empenho",
		"Código Empenho",
		"Código Categoria de Despesa",
		"Categoria de Despesa",
		"Código Grupo de Despesa",
		"Grupo de Despesa",
		"Código Modalidade de Aplicação",
		"Modalidade de Aplicação",
		"Código Elemento de Despesa",
		"Elemento de Despesa",
		"Código SubElemento de Despesa",
		"SubElemento de Despesa",
		"Descrição",
		"Quantidade",
		"Valor Unitário",
		"Valor Total",
		"Sequencial",
		"Valor Atual",
	},
	service.DespesasItemEmpenhoHistorico: {
		"Id Empenho",
		"Código Empenho",
		"Sequencial",
		"Tipo Operação",
		"Data Operação",
		"Quantidade Item",
		"Valor Unitário Item",
		"Valor Total Item",
	},
	service.DespesasExecucao: {
		"Ano e mês do lançamento",
		"Código Órgão Superior",
		"Nome Órgão Superior",
		"Código Órgão Subordinado",
		"Nome Órgão Subordinado",
		"Código Unidade Gestora",
		"Nome Unidade Gestora",
		"Código Gestão",
		"Nome Gestão",
		"Código Ação",
		"Nome Ação",
		"Código Plano Orçamentário",
		"Plano Orçamentário",
		"UF",
		"Município",
		"Código Autor Emenda",
		"Nome Autor Emenda",
		"Código Categoria Econômica",
		"Nome Categoria Econômica",
		"Código Grupo de Despesa",
		"Nome Grupo de Despesa",
		"Código Elemento de Despesa",
		"Nome Elemento de Despesa",
		"Código Modalidade da Despesa",
		"Modalidade da Despesa",
		"Valor Empenhado (R$)",
		"Valor Liquidado (R$)",
		"Valor Pago (R$)",
		"Valor Restos a Pagar Inscritos (R$)",
		"Valor Restos a Pagar Cancelado (R$)",
		"Valor Restos a Pagar Pagos (R$)",
	},
}

// Validates if the data type is supported for transformation
func validateDataTypeForTransformation(dfType service.DataType) error {
	if _, ok := columnsForDataType[dfType]; !ok {
		return fmt.Errorf("unsupported data type for transformation: %v", dfType)
	}
	return nil
}

func SelectDataframeColumns(df dataframe.DataFrame, dfType service.DataType) (dataframe.DataFrame, error) {

	if err := validateDataTypeForTransformation(dfType); err != nil {
		return dataframe.DataFrame{}, err
	}

	selectedCols := columnsForDataType[dfType]
	result := df.Select(selectedCols)

	if result.Error() != nil {
		return dataframe.DataFrame{}, fmt.Errorf("error selecting columns: %v", result.Error())
	}

	return result, nil
}

/*
Finds rows in the dataframe that match the given codes in the specified column and sends the resulting dataframe to the provided channel.
(This function runs as a goroutine and signals completion via the WaitGroup.)
*/
func FindRows(df dataframe.DataFrame, dfType service.DataType, codes []string, codeColumn string, date string, ch chan service.MatchingDataframe, wg *sync.WaitGroup, appLogger *logger.Logger) {
	const component = "DataFilter"
	defer wg.Done()

	appLogger.Debug(component, "Starting row search: date=%s type=%s column=%s codesCount=%d", date, service.DataTypeNames[dfType], codeColumn, len(codes))

	filter := dataframe.F{
		Colname:    codeColumn,
		Comparator: series.In,
		Comparando: codes,
	}

	matchingRows := df.Filter(
		filter,
	)

	if df.Error() != nil {
		appLogger.Error(component, "DataFrame filter error: date=%s type=%s error=%v", date, service.DataTypeNames[dfType], df.Error())
		return
	}

	appLogger.Info(component, "Row search completed: date=%s type=%s matchingRows=%d", date, service.DataTypeNames[dfType], matchingRows.Nrow())
	if matchingRows.Nrow() > 0 {
		ch <- service.MatchingDataframe{Dataframe: matchingRows, Type: dfType}
	}
}

// debugSaveDf writes df to tmp/debug/<dfType>_<sanitizedColumn>_<timestamp>.csv.
// It is a no-op when debug is false, the dataframe is empty, or the file cannot be created.
func debugSaveDf(df dataframe.DataFrame, dfType service.DataType, codeColumn string, debug bool) {
	if !debug || df.Nrow() == 0 {
		return
	}
	_ = os.MkdirAll("tmp/debug", os.ModePerm)
	col := strings.NewReplacer(" ", "_", "/", "-").Replace(codeColumn)
	name := fmt.Sprintf("tmp/debug/%d_%s_%s.csv", dfType, col, time.Now().Format("20060102_150405"))
	f, err := os.Create(name)
	if err != nil {
		return
	}
	defer f.Close()
	err = df.WriteCSV(f, dataframe.WriteHeader(true))
	if err != nil {
		fmt.Println("Error writing CSV:", err)
		return
	}
}

/*
Finds rows in the dataframe that match the given codes in the specified column synchronously.
*/
func FindRowsSync(df dataframe.DataFrame, dfType service.DataType, codes []string, codeColumn string, debug bool) dataframe.DataFrame {

	filter := dataframe.F{
		Colname:    codeColumn,
		Comparator: series.In,
		Comparando: codes,
	}

	matchingRows := df.Filter(
		filter,
	)

	if matchingRows.Error() != nil {
		return dataframe.DataFrame{}
	}
	if matchingRows.Nrow() > 0 {
		matchingDf, err := SelectDataframeColumns(matchingRows, dfType)
		if err != nil {
			fmt.Println("Error selecting columns:", err)
			return dataframe.DataFrame{}
		}
		debugSaveDf(matchingDf, dfType, codeColumn, debug)
		return matchingDf
	}

	return dataframe.DataFrame{}
}

func FilterExtractionByColumn(extraction service.OutputExpensesExtractionFiles, targetDataservice []service.DataType, codes []string, matchColumn string, chToRelease chan service.MatchingDataframe, wg *sync.WaitGroup, appLogger *logger.Logger) {
	const component = "ExtractionFilter"
	appLogger.Info(component, "Starting extraction filter: column=%s", matchColumn)
	for _, dt := range targetDataservice {
		if p, ok := extraction.Files[dt]; ok {
			appLogger.Debug(component, "Processing file: date=%s type=%s path=%s", extraction.Date, service.DataTypeNames[dt], p)
			df, err := filesystem.OpenFileAndDecode(p)
			if err != nil {
				appLogger.Warn(component, "Failed to open/decode file: date=%s type=%s path=%s error=%v", extraction.Date, service.DataTypeNames[dt], p, err)
				continue
			}
			wg.Add(1)
			go FindRows(df, dt, codes, matchColumn, extraction.Date, chToRelease, wg, appLogger)
		}
	}

	appLogger.Info(component, "Extraction filter completed")
}
