package transparency

import (
	"archive/zip"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/farxc/transparency_wrapper/internal/logger"
	"github.com/go-gota/gota/dataframe"
	"github.com/go-gota/gota/series"
	"golang.org/x/text/encoding/charmap"
)

var PORTAL_TRANSPARENCIA_URL = "https://portaldatransparencia.gov.br/download-de-dados/despesas/"

type DownloadResult struct {
	Success    bool
	OutputPath string
}

type ExtractionResult struct {
	Success   bool
	Data      DataType
	OutputDir string
}

type MatchingDataframe struct {
	Dataframe dataframe.DataFrame
	Type      DataType
}

type CommitmentItems struct {
	CommitmentCode string
	ItemsDf        dataframe.DataFrame
}

type DayExtraction struct {
	Date  string
	Files map[DataType]string
}

var dataTypeSuffix = map[DataType]string{
	DespesasEmpenho:                      DespesasEmpenhoDataType,
	DespesasItemEmpenho:                  DespesasItemEmpenhoDataType,
	DespesasItemEmpenhoHistorico:         DespesasItemEmpenhoHistoricoDataType,
	DespesasLiquidacao:                   DespesasLiquidacaoDataType,
	DespesasPagamento:                    DespesasPagamentoDataType,
	DespesasLiquidacaoEmpenhosImpactados: DespesasLiquidacaoEmpenhosImpactadosDataType,
	DespesasPagamentoEmpenhosImpactados:  DespesasPagamentoEmpenhosImpactadosDataType,
	DespesasPagamentoListaBancos:         DespesasPagamentoListaBancosDataType,
	DespesasPagamentoListaFaturas:        DespesasPagamentoListaFaturasDataType,
	DespesasPagamentoListaPrecatorios:    DespesasPagamentoListaPrecatoriosDataType,
}

var notUsedFiles = []DataType{
	DespesasPagamentoListaBancos,
	DespesasPagamentoListaFaturas,
	DespesasPagamentoListaPrecatorios,
}
var columnsForDataType = map[DataType][]string{
	DespesasPagamento: {
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
	DespesasPagamentoEmpenhosImpactados: {
		"Código Pagamento",
		"Código Empenho",
		"Código Natureza Despesa Completa",
		"Subitem",
		"Valor Pago (R$)",
		"Valor Restos a Pagar Inscritos (R$)",
		"Valor Restos a Pagar Cancelados (R$)",
		"Valor Restos a Pagar Pagos (R$)",
	},
	DespesasLiquidacao: {
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
	DespesasLiquidacaoEmpenhosImpactados: {
		"Código Liquidação",
		"Código Empenho",
		"Código Natureza Despesa Completa",
		"Subitem",
		"Valor Liquidado (R$)",
		"Valor Restos a Pagar Inscritos (R$)",
		"Valor Restos a Pagar Cancelado (R$)",
		"Valor Restos a Pagar Liquidados (R$)",
	},
	DespesasEmpenho: {
		"Código Empenho",
		"Código Empenho Resumido",
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
	DespesasItemEmpenho: {
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
	DespesasItemEmpenhoHistorico: {
		"Código Empenho",
		"Sequencial",
		"Tipo Operação",
		"Data Operação",
		"Quantidade Item",
		"Valor Unitário Item",
		"Valor Total Item",
	},
}

var DataTypeNames = map[DataType]string{
	DespesasEmpenho:                      "Despesas Empenho",
	DespesasItemEmpenho:                  "Despesas Item Empenho",
	DespesasItemEmpenhoHistorico:         "Despesas Item Empenho Histórico",
	DespesasLiquidacao:                   "Despesas Liquidação",
	DespesasPagamento:                    "Despesas Pagamento",
	DespesasLiquidacaoEmpenhosImpactados: "Despesas Liquidação Empenhos Impactados",
	DespesasPagamentoEmpenhosImpactados:  "Despesas Pagamento Empenhos Impactados",
	DespesasPagamentoListaBancos:         "Despesas Pagamento Lista Bancos",
	DespesasPagamentoListaFaturas:        "Despesas Pagamento Lista Faturas",
	DespesasPagamentoListaPrecatorios:    "Despesas Pagamento Lista Precatórios",
}

func isFileUsed(filename string) bool {

	for _, v := range notUsedFiles {
		if strings.HasSuffix(filename, dataTypeSuffix[v]) {
			return false
		}
	}

	return true
}

func FetchData(downloadUrl string, date string, logger *logger.Logger) DownloadResult {
	const component = "Downloader"
	output_path := "tmp/zips/despesas_" + date + ".zip"

	logger.Debug(component, "Starting download for date=%s url=%s", date, downloadUrl)

	client := &http.Client{}

	client.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		req.Header.Add("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3")
		return nil
	}
	// Create a new request with a custom User-Agent header
	req, err := http.NewRequest(http.MethodGet, downloadUrl, nil)
	if err != nil {
		logger.Error(component, "Failed to create HTTP request: date=%s error=%v", date, err)
		return DownloadResult{Success: false}
	}

	resp, err := client.Do(req)

	if err != nil {
		logger.Error(component, "HTTP request failed: date=%s error=%v", date, err)
		return DownloadResult{Success: false}
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		logger.Warn(component, "Non-OK HTTP response: date=%s status=%s statusCode=%d", date, resp.Status, resp.StatusCode)
		return DownloadResult{Success: false}
	}

	out, err := os.Create(output_path)

	if err != nil {
		logger.Error(component, "Failed to create output file: date=%s path=%s error=%v", date, output_path, err)
		return DownloadResult{Success: false}
	}
	defer out.Close()

	bytesWritten, err := io.Copy(out, resp.Body)
	if err != nil {
		logger.Error(component, "Failed to write data to file: date=%s error=%v", date, err)
		return DownloadResult{Success: false}
	}

	logger.Info(component, "Download completed: date=%s path=%s size=%d bytes", date, output_path, bytesWritten)
	return DownloadResult{Success: true, OutputPath: output_path}
}

func UnzipFile(zipPath string, destDir string, appLogger *logger.Logger) ExtractionResult {
	const component = "Unzipper"

	if destDir == "" {
		destDir = "tmp/data"
	}

	appLogger.Debug(component, "Starting extraction: zipPath=%s destDir=%s", zipPath, destDir)

	err := os.MkdirAll(destDir, os.ModePerm)
	if err != nil {
		appLogger.Error(component, "Failed to create directory: destDir=%s error=%v", destDir, err)
		return ExtractionResult{Success: false}
	}

	r, err := zip.OpenReader(zipPath)
	if err != nil {
		appLogger.Error(component, "Failed to open zip file: zipPath=%s error=%v", zipPath, err)
		return ExtractionResult{Success: false}
	}
	defer r.Close()

	extractedCount := 0
	skippedCount := 0

	for _, f := range r.File {
		filePath := filepath.Join(destDir, f.Name)

		if !strings.HasPrefix(filePath, filepath.Clean(destDir)+string(os.PathSeparator)) {
			appLogger.Error(component, "Invalid file path detected (possible zip slip): file=%s", f.Name)
			return ExtractionResult{Success: false}
		}

		if !isFileUsed(f.Name) {
			skippedCount++
			appLogger.Debug(component, "Skipping unused file: file=%s", f.Name)
			continue
		}

		destFile, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, f.Mode())
		if err != nil {
			appLogger.Error(component, "Failed to create destination file: file=%s error=%v", filePath, err)
			return ExtractionResult{Success: false}
		}
		defer destFile.Close()

		zippedFile, err := f.Open()
		if err != nil {
			appLogger.Error(component, "Failed to open zipped file: file=%s error=%v", f.Name, err)
			return ExtractionResult{Success: false}
		}
		defer zippedFile.Close()

		if _, err := io.Copy(destFile, zippedFile); err != nil {
			appLogger.Error(component, "Failed to extract file: file=%s error=%v", f.Name, err)
			return ExtractionResult{Success: false}
		}
		extractedCount++
	}

	appLogger.Info(component, "Extraction completed: destDir=%s extractedFiles=%d skippedFiles=%d", destDir, extractedCount, skippedCount)
	return ExtractionResult{Success: true, Data: DespesasEmpenho, OutputDir: destDir}
}

func BuildFilesForDate(date, dir string) map[DataType]string {
	m := make(map[DataType]string, len(dataTypeSuffix))

	for dt, suffix := range dataTypeSuffix {
		m[dt] = filepath.Join(dir, date+suffix)
	}
	return m
}

func OpenFileAndDecode(path string) (dataframe.DataFrame, error) {
	const component = "FileDecoder"

	file, err := os.Open(path)
	if err != nil {
		return dataframe.DataFrame{}, fmt.Errorf("failed to open file %s: %v", path, err)
	}

	defer file.Close()

	// Using Windows1252 because it is the encoding used by the original CSV files
	decoded := charmap.Windows1252.NewDecoder().Reader(file)
	df := dataframe.ReadCSV(decoded, dataframe.WithDelimiter(';'), dataframe.WithLazyQuotes(true))
	// If dataframe is empty return
	if df.Nrow() == 0 {
		return dataframe.DataFrame{}, fmt.Errorf("dataframe is empty")
	}

	return df, df.Error()
}

func dfRowToCommitment(df dataframe.DataFrame, rowIdx int) Commitment {
	getStr := func(col string) string {
		if idx := df.Names(); containsString(idx, col) {
			return df.Col(col).Elem(rowIdx).String()
		}
		return ""
	}

	return Commitment{
		CommitmentCode:                getStr("Código Empenho"),
		ResumedCommitmentCode:         getStr("Código Empenho Resumido"),
		EmitionDate:                   getStr("Data Emissão"),
		Type:                          getStr("Tipo Empenho"),
		ManagementCode:                getStr("Código Gestão"),
		ManagementName:                getStr("Gestão"),
		Process:                       getStr("Processo"),
		FavoredName:                   getStr("Favorecido"),
		FavoredCode:                   getStr("Código Favorecido"),
		ExpenseNature:                 getStr("Elemento de Despesa"),
		CompleteExpenseNature:         getStr("Natureza de Despesa Completa"),
		BudgetPlan:                    getStr("Plano Orçamentário"),
		CommitmentOriginalValue:       getStr("Valor Original do Empenho"),
		CommitmentValueConvertedToBRL: getStr("Valor do Empenho Convertido pra R$"),
		ConversionValueUsed:           getStr("Valor Utilizado na Conversão"),
		Items:                         []CommitmentItem{},
	}
}

func dfRowToLiquidation(df dataframe.DataFrame, rowIdx int) Liquidation {
	getStr := func(col string) string {
		if containsString(df.Names(), col) {
			return df.Col(col).Elem(rowIdx).String()
		}
		return ""
	}

	return Liquidation{
		LiquidationCode:        getStr("Código Liquidação"),
		LiquidationCodeResumed: getStr("Código Liquidação Resumido"),
		LiquidationEmitionDate: getStr("Data Emissão"),
		DocumentCodeType:       getStr("Código Tipo Documento"),
		DocumentType:           getStr("Tipo Documento"),
		FavoredCode:            getStr("Código Favorecido"),
		FavoredName:            getStr("Favorecido"),
		Observation:            getStr("Observação"),
	}
}

func dfRowToPayment(df dataframe.DataFrame, rowIdx int) Payment {
	getStr := func(col string) string {
		if containsString(df.Names(), col) {
			return df.Col(col).Elem(rowIdx).String()
		}
		return ""
	}

	return Payment{
		PaymentCode:           getStr("Código Pagamento"),
		PaymentCodeResumed:    getStr("Código Pagamento Resumido"),
		PaymentEmitionDate:    getStr("Data Emissão"),
		DocumentCodeType:      getStr("Código Tipo Documento"),
		DocumentType:          getStr("Tipo Documento"),
		FavoredCode:           getStr("Código Favorecido"),
		FavoredName:           getStr("Favorecido"),
		ExtraBudgetary:        getStr("Extraorçamentário"),
		Process:               getStr("Processo"),
		OriginalPaymentValue:  getStr("Valor Original do Pagamento"),
		ConvertedPaymentValue: getStr("Valor do Pagamento Convertido pra R$"),
		ConversionUsedValue:   getStr("Valor Utilizado na Conversão"),
	}
}

func dfRowToCommitmentItem(df dataframe.DataFrame, rowIdx int) CommitmentItem {
	getStr := func(col string) string {
		if containsString(df.Names(), col) {
			return df.Col(col).Elem(rowIdx).String()
		}
		return ""
	}
	getInt := func(col string) int {
		if idx := df.Names(); containsString(idx, col) {
			val, err := df.Col(col).Elem(rowIdx).Int()
			if err != nil {
				return 0
			}
			return val
		}
		return 0
	}

	return CommitmentItem{
		ExpenseCategory:    getStr("Categoria de Despesa"),
		ExpenseGroup:       getStr("Grupo de Despesa"),
		AplicationModality: getStr("Modalidade de Aplicação"),
		ExpenseElement:     getStr("Elemento de Despesa"),
		Description:        getStr("Descrição"),
		Sequential:         getInt("Sequencial"),
		Quantity:           getStr("Quantidade"),
		UnitPrice:          getStr("Valor Unitário"),
		CurrentValue:       getStr("Valor Atual"),
		TotalPrice:         getStr("Valor Total"),
		History:            []CommitmentItemHistory{},
	}
}

func dfRowToCommitmentItemHistory(df dataframe.DataFrame, rowIdx int) CommitmentItemHistory {
	getStr := func(col string) string {
		if containsString(df.Names(), col) {
			return df.Col(col).Elem(rowIdx).String()
		}
		return ""
	}
	getInt := func(col string) int {
		if idx := df.Names(); containsString(idx, col) {
			val, err := df.Col(col).Elem(rowIdx).Int()
			if err != nil {
				return 0
			}
			return val
		}
		return 0
	}

	return CommitmentItemHistory{
		OperationType:  getStr("Tipo Operação"),
		OperationDate:  getStr("Data Operação"),
		Sequential:     getInt("Sequencial"),
		ItemQuantity:   getStr("Quantidade Item"),
		ItemUnitPrice:  getStr("Valor Unitário Item"),
		ItemTotalPrice: getStr("Valor Total Item"),
	}
}

func dfRowToPaymentImpactedCommitment(df dataframe.DataFrame, rowIdx int) PaymentImpactedCommitment {
	getStr := func(col string) string {
		if containsString(df.Names(), col) {
			return df.Col(col).Elem(rowIdx).String()
		}
		return ""
	}
	return PaymentImpactedCommitment{
		PaymentCode:                getStr("Código Pagamento"),
		CommitmentCode:             getStr("Código Empenho"),
		CompleteExpenseNature:      getStr("Natureza de Despesa Completa"),
		Subitem:                    getStr("Subitem"),
		PaidValueBRL:               getStr("Valor Pago (R$)"),
		RegisteredPayablesValueBRL: getStr("Valor Restos a Pagar Inscritos (R$)"),
		CanceledPayablesValueBRL:   getStr("Valor Restos a Pagar Cancelados (R$)"),
		OutstandingValuePaidBRL:    getStr("Valor Restos a Pagar Pagos (R$)"),
	}
}

func dfRowToLiquidationImpactedCommitment(df dataframe.DataFrame, rowIdx int) LiquidationImpactedCommitment {
	getStr := func(col string) string {
		if containsString(df.Names(), col) {
			return df.Col(col).Elem(rowIdx).String()
		}
		return ""
	}
	return LiquidationImpactedCommitment{
		LiquidationCode:               getStr("Código Liquidação"),
		CommitmentCode:                getStr("Código Empenho"),
		CompleteExpenseNature:         getStr("Natureza de Despesa Completa"),
		Subitem:                       getStr("Subitem"),
		LiquidatedValueBRL:            getStr("Valor Liquidado (R$)"),
		RegisteredPayablesValueBRL:    getStr("Valor Restos a Pagar Inscritos (R$)"),
		CanceledPayablesValueBRL:      getStr("Valor Restos a Pagar Cancelado (R$)"),
		OutstandingValueLiquidatedBRL: getStr("Valor Restos a Pagar Liquidados (R$)"),
	}
}

func containsString(slice []string, s string) bool {
	for _, v := range slice {
		if v == s {
			return true
		}
	}
	return false
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

// Returns whether the given DataType is a main data type (Commitment, Liquidation, Payment) and its main code column name
func getMainDataTypeColumn(dfType DataType) string {
	switch dfType {
	case DespesasEmpenho:
		return "Código Empenho"
	case DespesasLiquidacao:
		return "Código Liquidação"
	case DespesasPagamento:
		return "Código Pagamento"
	default:
		return ""
	}
}

// Validates if the data type is supported for transformation
func validateDataTypeForTransformation(dfType DataType) error {
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

// Extracts the main code from the dataframe for the given data type
func extractMainCode(df dataframe.DataFrame, dfType DataType) string {
	codeColumn := getMainDataTypeColumn(dfType)
	if codeColumn != "" && df.Nrow() > 0 {
		return df.Col(codeColumn).Elem(0).String()
	}
	return ""
}

func selectDataframeColumns(df dataframe.DataFrame, dfType DataType) (dataframe.DataFrame, error) {

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
func findRows(df dataframe.DataFrame, dfType DataType, codes []string, codeColumn string, date string, ch chan MatchingDataframe, wg *sync.WaitGroup, appLogger *logger.Logger) {
	const component = "DataFilter"
	defer wg.Done()

	appLogger.Debug(component, "Starting row search: date=%s type=%s column=%s codesCount=%d", date, DataTypeNames[dfType], codeColumn, len(codes))

	filter := dataframe.F{
		Colname:    codeColumn,
		Comparator: series.In,
		Comparando: codes,
	}

	matchingRows := df.Filter(
		filter,
	)

	if df.Error() != nil {
		appLogger.Error(component, "DataFrame filter error: date=%s type=%s error=%v", date, DataTypeNames[dfType], df.Error())
		return
	}

	appLogger.Info(component, "Row search completed: date=%s type=%s matchingRows=%d", date, DataTypeNames[dfType], matchingRows.Nrow())
	if matchingRows.Nrow() > 0 {
		ch <- MatchingDataframe{Dataframe: matchingRows, Type: dfType}
	}
}

/*
Finds rows in the dataframe that match the given codes in the specified column synchronously.
*/
func findRowsSync(df dataframe.DataFrame, dfType DataType, codes []string, codeColumn string) dataframe.DataFrame {

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
		matchingDf, err := selectDataframeColumns(matchingRows, dfType)
		if err != nil {
			return dataframe.DataFrame{}
		}
		return matchingDf
	}

	return dataframe.DataFrame{}
}

func filterExtractionByColumn(extraction DayExtraction, targetDataTypes []DataType, codes []string, matchColumn string, chToRelease chan MatchingDataframe, wg *sync.WaitGroup, appLogger *logger.Logger) {
	const component = "ExtractionFilter"
	appLogger.Info(component, "Starting extraction filter: column=%s", matchColumn)
	for _, dt := range targetDataTypes {
		if p, ok := extraction.Files[dt]; ok {
			appLogger.Debug(component, "Processing file: date=%s type=%s path=%s", extraction.Date, DataTypeNames[dt], p)
			df, err := OpenFileAndDecode(p)
			if err != nil {
				appLogger.Warn(component, "Failed to open/decode file: date=%s type=%s path=%s error=%v", extraction.Date, DataTypeNames[dt], p, err)
				continue
			}
			wg.Add(1)
			go findRows(df, dt, codes, matchColumn, extraction.Date, chToRelease, wg, appLogger)
		}
	}

	appLogger.Info(component, "Extraction filter completed")
}

// attachItemsAndHistoryToCommitment attaches commitment items and their history to a commitment
func attachItemsAndHistoryToCommitment(
	commitment *Commitment,
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

		item := dfRowToCommitmentItem(itemsDf, j)

		// Attach history to this item
		if historyDf.Nrow() > 0 {
			itemSequential := itemsDf.Col("Sequencial").Elem(j).String()
			for k := 0; k < historyDf.Nrow(); k++ {
				histCommitmentCode := historyDf.Col("Código Empenho").Elem(k).String()
				histSequential := historyDf.Col("Sequencial").Elem(k).String()
				if histCommitmentCode == commitment.CommitmentCode && histSequential == itemSequential {
					history := dfRowToCommitmentItemHistory(historyDf, k)
					item.History = append(item.History, history)
				}
			}
		}

		commitment.Items = append(commitment.Items, item)
	}
}

func BuildCommitmentPayload(extraction DayExtraction, unitsCode []string, includeItemsAndHistory bool) (*CommitmentPayload, error) {

	const component = "CommitmentExtractor"
	extractionDate, err := time.Parse("20060102", extraction.Date)
	if err != nil {
		return nil, fmt.Errorf("invalid extraction date format: %v", err)
	}
	formattedDate := extractionDate.Format("2006-01-02")

	if p, ok := extraction.Files[DespesasEmpenho]; ok {
		df, err := OpenFileAndDecode(p)
		if err != nil {
			return nil, err
		}

		commitmentsMatchingDf := findRowsSync(df, DespesasEmpenho, unitsCode, "Código Unidade Gestora")

		if commitmentsMatchingDf.Nrow() == 0 {
			return nil, fmt.Errorf("no matching commitments found for extraction date %s", extractionDate)
		}
		// Prepare to hold items and history dataframes if needed
		var commitmentsItemsMatchingDf dataframe.DataFrame
		var commitmentItemsHistoryDf dataframe.DataFrame

		if includeItemsAndHistory {
			// Get commitment codes for sub-extraction
			ugsCommitments := commitmentsMatchingDf.Col("Código Empenho").Records()

			if p, ok := extraction.Files[DespesasItemEmpenho]; ok {
				df, err = OpenFileAndDecode(p)
				if err != nil {
					return nil, err
				}
				commitmentsItemsMatchingDf = findRowsSync(df, DespesasItemEmpenho, ugsCommitments, "Código Empenho")
			}

			if p, ok := extraction.Files[DespesasItemEmpenhoHistorico]; ok {
				df, err = OpenFileAndDecode(p)
				if err != nil {
					return nil, err
				}
				commitmentItemsHistoryDf = findRowsSync(df, DespesasItemEmpenhoHistorico, ugsCommitments, "Código Empenho")
			}
		}

		// Build the hierarchical JSON structure
		payload := &CommitmentPayload{
			ExtractionDate:  formattedDate,
			UnitCommitments: []UnitCommitments{},
		}

		// Group by Unit Code
		unitMap := make(map[string]*UnitCommitments)

		// Helper to get or create unit entry
		getOrCreateUnit := func(ugCode, ugName string) *UnitCommitments {
			if _, exists := unitMap[ugCode]; !exists {
				unitMap[ugCode] = &UnitCommitments{
					UgCode:       ugCode,
					UgName:       ugName,
					Commitments:  []Commitment{},
					Liquidations: []Liquidation{},
					Payments:     []Payment{},
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

			commitment := dfRowToCommitment(commitmentsMatchingDf, i)

			// Attach items and history if needed
			if includeItemsAndHistory {
				attachItemsAndHistoryToCommitment(&commitment, commitmentsItemsMatchingDf, commitmentItemsHistoryDf)
			}

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
func ExtractData(extraction DayExtraction, unitsCode []string, appLogger *logger.Logger) (*CommitmentPayload, error) {
	const component = "DataExtractor"
	var wg sync.WaitGroup

	extractionDate, err := time.Parse("20060102", extraction.Date)
	if err != nil {
		return nil, fmt.Errorf("invalid extraction date format: %v", err)
	}
	formattedDate := extractionDate.Format("2006-01-02")

	appLogger.Info(component, "Starting data extraction: date=%s unitsCount=%d", formattedDate, len(unitsCode))

	// Channel for collect DataFrames based in Unit Codes
	ugMatches := make(chan MatchingDataframe, 3)

	// Channel for collect DataFrames based in Commitment Codes
	commitmentMatches := make(chan MatchingDataframe, 3)

	hasUgCodeAsColumn := []DataType{
		DespesasEmpenho,
		DespesasLiquidacao,
		DespesasPagamento,
	}

	hasCommitmentCodeAsColumn := []DataType{
		DespesasItemEmpenho,
		DespesasItemEmpenhoHistorico,
	}

	appLogger.Debug(component, "Phase 1: Filtering by UG codes: date=%s", extractionDate)
	// First, find all Commitments based in Unit Codes
	filterExtractionByColumn(extraction, hasUgCodeAsColumn, unitsCode, "Código Unidade Gestora", ugMatches, &wg, appLogger)

	wg.Wait()
	close(ugMatches)

	// Collect DataFrames by type
	empenhosDf := dataframe.New()
	liquidacoesDf := dataframe.New()
	pagamentosDf := dataframe.New()
	for extracted := range ugMatches {
		transformedDf, err := selectDataframeColumns(extracted.Dataframe, extracted.Type)

		if err != nil {
			appLogger.Error(component, "DataFrame transformation error: date=%s type=%s error=%v", extractionDate, DataTypeNames[extracted.Type], err)
			continue
		}

		appLogger.Debug(component, "DataFrame transformed: date=%s type=%s rows=%d", extractionDate, DataTypeNames[extracted.Type], transformedDf.Nrow())

		switch extracted.Type {
		case DespesasEmpenho:
			empenhosDf = transformedDf
		case DespesasLiquidacao:
			liquidacoesDf = transformedDf
		case DespesasPagamento:
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

	// Only extract commitment items if we have commitments
	itemsDf := dataframe.DataFrame{}
	historyDf := dataframe.DataFrame{}

	if empenhosDf.Nrow() > 0 {
		// Get commitment codes for sub-extraction
		ugsCommitments := empenhosDf.Col("Código Empenho").Records()
		appLogger.Debug(component, "Phase 2: Extracting commitment items: date=%s commitmentCodes=%d", extractionDate, len(ugsCommitments))

		// Extract commitment items and history
		filterExtractionByColumn(extraction, hasCommitmentCodeAsColumn,
			ugsCommitments, "Código Empenho", commitmentMatches, &wg, appLogger)
		wg.Wait()
		close(commitmentMatches)

		for extracted := range commitmentMatches {
			transformedDf, err := selectDataframeColumns(extracted.Dataframe, extracted.Type)
			if err != nil {
				appLogger.Error(component, "Commitment items transformation error: date=%s type=%s error=%v", extractionDate, DataTypeNames[extracted.Type], err)
				continue
			}

			switch extracted.Type {
			case DespesasItemEmpenho:
				itemsDf = transformedDf
			case DespesasItemEmpenhoHistorico:
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
	payload := &CommitmentPayload{
		ExtractionDate:  formattedDate,
		UnitCommitments: []UnitCommitments{},
	}

	// Group by Unit Code
	unitMap := make(map[string]*UnitCommitments)

	// Helper to get or create unit entry
	getOrCreateUnit := func(ugCode, ugName string) *UnitCommitments {
		if _, exists := unitMap[ugCode]; !exists {
			unitMap[ugCode] = &UnitCommitments{
				UgCode:       ugCode,
				UgName:       ugName,
				Commitments:  []Commitment{},
				Liquidations: []Liquidation{},
				Payments:     []Payment{},
			}
		}
		// Update name if it was empty
		if unitMap[ugCode].UgName == "" && ugName != "" {
			unitMap[ugCode].UgName = ugName
		}
		return unitMap[ugCode]
	}

	// Process commitments (empenhos)
	for i := 0; i < empenhosDf.Nrow(); i++ {
		ugCode := empenhosDf.Col("Código Unidade Gestora").Elem(i).String()
		ugName := empenhosDf.Col("Unidade Gestora").Elem(i).String()
		unit := getOrCreateUnit(ugCode, ugName)

		commitment := dfRowToCommitment(empenhosDf, i)

		// Attach items to this commitment
		if itemsDf.Nrow() > 0 {
			for j := 0; j < itemsDf.Nrow(); j++ {
				itemCommitmentCode := itemsDf.Col("Código Empenho").Elem(j).String()
				if itemCommitmentCode == commitment.CommitmentCode {
					item := dfRowToCommitmentItem(itemsDf, j)

					// Attach history to this item
					if historyDf.Nrow() > 0 {
						itemSequential := itemsDf.Col("Sequencial").Elem(j).String()
						for k := 0; k < historyDf.Nrow(); k++ {
							histCommitmentCode := historyDf.Col("Código Empenho").Elem(k).String()
							histSequential := historyDf.Col("Sequencial").Elem(k).String()
							if histCommitmentCode == commitment.CommitmentCode && histSequential == itemSequential {
								history := dfRowToCommitmentItemHistory(historyDf, k)
								item.History = append(item.History, history)
							}
						}
					}

					commitment.Items = append(commitment.Items, item)
				}
			}
		}

		unit.Commitments = append(unit.Commitments, commitment)
	}

	// Process liquidations
	for i := 0; i < liquidacoesDf.Nrow(); i++ {
		ugCode := liquidacoesDf.Col("Código Unidade Gestora").Elem(i).String()
		ugName := liquidacoesDf.Col("Unidade Gestora").Elem(i).String()
		unit := getOrCreateUnit(ugCode, ugName)

		liquidation := dfRowToLiquidation(liquidacoesDf, i)
		unit.Liquidations = append(unit.Liquidations, liquidation)
	}

	// Process payments
	for i := 0; i < pagamentosDf.Nrow(); i++ {
		ugCode := pagamentosDf.Col("Código Unidade Gestora").Elem(i).String()
		ugName := pagamentosDf.Col("Unidade Gestora").Elem(i).String()
		unit := getOrCreateUnit(ugCode, ugName)

		payment := dfRowToPayment(pagamentosDf, i)
		unit.Payments = append(unit.Payments, payment)
	}

	// Convert map to slice
	for _, unit := range unitMap {
		payload.UnitCommitments = append(payload.UnitCommitments, *unit)
	}

	appLogger.Info(component, "Extraction completed: date=%s unitsProcessed=%d", extractionDate, len(payload.UnitCommitments))
	return payload, nil
}
