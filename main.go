package main

import (
	"archive/zip"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/go-gota/gota/dataframe"
	"github.com/go-gota/gota/series"
	"golang.org/x/text/encoding/charmap"
)

// LogLevel represents the severity of a log message
type LogLevel int

const (
	LevelDebug LogLevel = iota
	LevelInfo
	LevelWarn
	LevelError
)

var logLevelNames = map[LogLevel]string{
	LevelDebug: "DEBUG",
	LevelInfo:  "INFO",
	LevelWarn:  "WARN",
	LevelError: "ERROR",
}

// Logger provides structured logging with levels
type Logger struct {
	minLevel LogLevel
	mu       sync.Mutex
}

var appLogger = &Logger{minLevel: LevelInfo}

// SetLogLevel sets the minimum log level
func (l *Logger) SetLogLevel(level LogLevel) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.minLevel = level
}

func (l *Logger) log(level LogLevel, component, message string, args ...interface{}) {
	if level < l.minLevel {
		return
	}

	timestamp := time.Now().Format("2006-01-02 15:04:05.000")
	levelStr := logLevelNames[level]
	formattedMsg := fmt.Sprintf(message, args...)

	l.mu.Lock()
	defer l.mu.Unlock()

	if component != "" {
		log.Printf("[%s] [%s] [%s] %s", timestamp, levelStr, component, formattedMsg)
	} else {
		log.Printf("[%s] [%s] %s", timestamp, levelStr, formattedMsg)
	}
}

// Debug logs a debug message
func (l *Logger) Debug(component, message string, args ...interface{}) {
	l.log(LevelDebug, component, message, args...)
}

// Info logs an info message
func (l *Logger) Info(component, message string, args ...interface{}) {
	l.log(LevelInfo, component, message, args...)
}

// Warn logs a warning message
func (l *Logger) Warn(component, message string, args ...interface{}) {
	l.log(LevelWarn, component, message, args...)
}

// Error logs an error message
func (l *Logger) Error(component, message string, args ...interface{}) {
	l.log(LevelError, component, message, args...)
}

// Fatal logs an error message and exits
func (l *Logger) Fatal(component, message string, args ...interface{}) {
	l.log(LevelError, component, message, args...)
	os.Exit(1)
}

type ProfilerStats struct {
	PeakGoroutines int
	PeakMemoryMB   uint64
}

type MemoryMonitor struct {
	mu    sync.Mutex
	stats ProfilerStats
	stop  chan struct{}
}

func NewMonitor() *MemoryMonitor {
	return &MemoryMonitor{
		stop: make(chan struct{}),
	}
}

func (m *MemoryMonitor) Start(interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				m.update()
			case <-m.stop:
				return
			}
		}

	}()
}

func (m *MemoryMonitor) update() {
	const component = "Monitor"

	var mStats runtime.MemStats
	runtime.ReadMemStats(&mStats)

	currentGoroutines := runtime.NumGoroutine()
	currentMemoryMB := mStats.Alloc / 1024 / 1024

	m.mu.Lock()
	defer m.mu.Unlock()

	if currentGoroutines > m.stats.PeakGoroutines {
		m.stats.PeakGoroutines = currentGoroutines
	}
	if currentMemoryMB > m.stats.PeakMemoryMB {
		m.stats.PeakMemoryMB = currentMemoryMB
	}

	appLogger.Debug(component, "goroutines=%d memoryMB=%d peakGoroutines=%d peakMemoryMB=%d", currentGoroutines, currentMemoryMB, m.stats.PeakGoroutines, m.stats.PeakMemoryMB)
}

func (m *MemoryMonitor) Stop() ProfilerStats {
	close(m.stop)
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.stats
}

type DataType int

const (
	DespesasEmpenho DataType = iota
	DespesasItemEmpenho
	DespesasItemEmpenhoHistorico
	DespesasLiquidacao
	DespesasPagamento
	DespesasPagamentoListaBancos
	DespesasPagamentoEmpenhosImpactados
	DespesasPagamentoFavoricidosFinais
	DespesasPagamentoListaFaturas
	DespesasPagamentoListaPrecatorios
	DespesasLiquidacaoEmpenhos
	DespesasLiquidacaoEmpenhosImpactados
)

const (
	DespesasEmpenhoDataType                      = "_Despesas_Empenho.csv"
	DespesasItemEmpenhoDataType                  = "_Despesas_ItemEmpenho.csv"
	DespesasItemEmpenhoHistoricoDataType         = "_Despesas_ItemEmpenhoHistorico.csv"
	DespesasLiquidacaoDataType                   = "_Despesas_Liquidacao.csv"
	DespesasPagamentoDataType                    = "_Despesas_Pagamento.csv"
	DespesasLiquidacaoEmpenhosImpactadosDataType = "_Despesas_Liquidacao_EmpenhosImpactados.csv"
	DespesasPagamentoEmpenhosImpactadosDataType  = "_Despesas_Pagamento_EmpenhosImpactados.csv"
	DespesasPagamentoListaBancosDataType         = "_Despesas_Pagamento_ListaBancos.csv"
	DespesasPagamentoListaFaturasDataType        = "_Despesas_Pagamento_ListaFaturas.csv"
	DespesasPagamentoListaPrecatoriosDataType    = "_Despesas_Pagamento_ListaPrecatorios.csv"
)

type DownloadResult struct {
	Success    bool
	OutputPath string
}

type ExtractionResult struct {
	Success   bool
	Data      DataType
	OutputDir string
}

type ExtractedDataframe struct {
	Dataframe dataframe.DataFrame
	Type      DataType
}

type CommitmentItems struct {
	CommitmentCode string
	ItemsDf        dataframe.DataFrame
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
	DespesasEmpenho: {
		"Código Empenho",
		"Código Empenho Resumido",
		"Data Emissão",
		"Tipo Empenho",
		"Código Unidade Gestora",
		"Unidade Gestora",
		"Código Gestão",
		"Gestão",
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

type CommitmentItemHistory struct {
	OperationType  string  `json:"operation_type"`
	OperationDate  string  `json:"operation_date"`
	ItemQuantity   int     `json:"item_quantity"`
	ItemUnitPrice  float32 `json:"item_unit_price"`
	ItemTotalPrice float32 `json:"item_total_price"`
}
type CommitmentItem struct {
	ExpenseCategory    string                  `json:"expense_category"`
	ExpenseGroup       string                  `json:"expense_group"`
	AplicationModality string                  `json:"aplication_modality"`
	ExpenseElement     string                  `json:"expense_element"`
	Description        string                  `json:"description"`
	Quantity           int                     `json:"quantity"`
	Sequential         int                     `json:"sequential"`
	UnitPrice          float32                 `json:"unit_price"`
	CurrentValue       float32                 `json:"current_value"`
	TotalPrice         float32                 `json:"total_price"`
	History            []CommitmentItemHistory `json:"history"`
}

type Commitment struct {
	CommitmentCode                string           `json:"commitment_code"`
	ResumedCommitmentCode         string           `json:"resumed_commitment_code"`
	EmitionDate                   string           `json:"emition_date"`
	Type                          string           `json:"type"`
	ManagementCode                string           `json:"management_code"`
	ManagementName                string           `json:"management_name"`
	FavoredCode                   string           `json:"favored_code"`
	ExpenseNature                 string           `json:"expense_nature"`
	CompleteExpenseNature         string           `json:"complete_expense_nature"`
	BudgetPlan                    string           `json:"budget_plan"`
	CommitmentOriginalValue       float32          `json:"commitment_original_value"`
	CommitmentValueConvertedToBRL float32          `json:"commitment_value_converted_to_brl"`
	ConversionValueUsed           float32          `json:"conversion_value_used"`
	Items                         []CommitmentItem `json:"items"`
}

type Liquidation struct {
	LiquidationCode        string `json:"liquidation_code"`
	LiquidationCodeResumed string `json:"liquidation_code_resumed"`
	LiquidationEmitionDate string `json:"liquidation_emition_date"`
	DocumentCodeType       string `json:"document_code_type"`
	DocumentType           string `json:"document_type"`
	FavoredCode            string `json:"favored_code"`
	FavoredName            string `json:"favored_name"`
	Observation            string `json:"observation"`
}

type Payment struct {
	PaymentCode           string `json:"payment_code"`
	PaymentCodeResumed    string `json:"payment_code_resumed"`
	PaymentEmitionDate    string `json:"payment_emition_date"`
	DocumentCodeType      string `json:"document_code_type"`
	DocumentType          string `json:"document_type"`
	FavoredCode           string `json:"favored_code"`
	FavoredName           string `json:"favored_name"`
	Observation           string `json:"observation"`
	ExtraBudgetary        string `json:"extra_budgetary"`
	Process               string `json:"process"`
	OriginalPaymentValue  string `json:"original_payment_value"`
	ConvertedPaymentValue string `json:"converted_payment_value"`
	ConversionUsedValue   string `json:"conversion_used_value"`
}
type UnitCommitments struct {
	UgCode       string        `json:"ug_code"`
	UgName       string        `json:"ug_name"`
	Commitments  []Commitment  `json:"commitments"`
	Liquidations []Liquidation `json:"liquidations"`
	Payments     []Payment     `json:"payments"`
}

type CommitmentPayload struct {
	UnitCommitments []UnitCommitments `json:"units"`
	ExtractionDate  string            `json:"extraction_date"`
}

var PORTAL_TRANSPARENCIA_URL = "https://portaldatransparencia.gov.br/download-de-dados/despesas/"

type DayExtraction struct {
	Date  string
	Files map[DataType]string
}

func createDirsIfNotExist(dirPath string) error {
	if _, err := os.Stat(dirPath); os.IsNotExist(err) {
		err := os.MkdirAll(dirPath, os.ModePerm)
		if err != nil {
			return err
		}
	}
	return nil
}

func clearTempDirs() {
	const component = "TempCleaner"
	dirs := []string{"tmp/zips", "tmp/data"}
	for _, dir := range dirs {
		err := os.RemoveAll(dir)
		if err != nil {
			appLogger.Warn(component, "Failed to clear temp dir: dir=%s error=%v", dir, err)
		} else {
			appLogger.Info(component, "Temp dir cleared: dir=%s", dir)
		}
	}
}

func fetchData(downloadUrl string, date string) DownloadResult {
	const component = "Downloader"
	output_path := "tmp/zips/despesas_" + date + ".zip"

	appLogger.Debug(component, "Starting download for date=%s url=%s", date, downloadUrl)

	client := &http.Client{}

	client.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		req.Header.Add("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3")
		return nil
	}
	// Create a new request with a custom User-Agent header
	req, err := http.NewRequest(http.MethodGet, downloadUrl, nil)
	if err != nil {
		appLogger.Error(component, "Failed to create HTTP request: date=%s error=%v", date, err)
		return DownloadResult{Success: false}
	}

	resp, err := client.Do(req)

	if err != nil {
		appLogger.Error(component, "HTTP request failed: date=%s error=%v", date, err)
		return DownloadResult{Success: false}
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		appLogger.Warn(component, "Non-OK HTTP response: date=%s status=%s statusCode=%d", date, resp.Status, resp.StatusCode)
		return DownloadResult{Success: false}
	}

	out, err := os.Create(output_path)

	if err != nil {
		appLogger.Error(component, "Failed to create output file: date=%s path=%s error=%v", date, output_path, err)
		return DownloadResult{Success: false}
	}
	defer out.Close()

	bytesWritten, err := io.Copy(out, resp.Body)
	if err != nil {
		appLogger.Error(component, "Failed to write data to file: date=%s error=%v", date, err)
		return DownloadResult{Success: false}
	}

	appLogger.Info(component, "Download completed: date=%s path=%s size=%d bytes", date, output_path, bytesWritten)
	return DownloadResult{Success: true, OutputPath: output_path}
}

func isFileUsed(filename string) bool {

	for _, v := range notUsedFiles {
		if strings.HasSuffix(filename, dataTypeSuffix[v]) {
			return false
		}
	}

	return true
}

func unzipFile(zipPath string, destDir string) ExtractionResult {
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

func buildFilesForDate(date, dir string) map[DataType]string {
	m := make(map[DataType]string, len(dataTypeSuffix))

	for dt, suffix := range dataTypeSuffix {
		m[dt] = filepath.Join(dir, date+suffix)
	}
	return m
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

func transformExpenses(df dataframe.DataFrame, dfType DataType) (dataframe.DataFrame, error) {
	selectedCols, ok := columnsForDataType[dfType]
	if !ok {
		return dataframe.DataFrame{}, fmt.Errorf("unsupported data type for transformation: %v", dfType)
	}

	result := df

	if dataframeContainsExpenseNatureColumns(df) {
		var err error
		result, err = concatCompleteExpenseNature(df)
		if err != nil {
			return dataframe.DataFrame{}, fmt.Errorf("error concatenating complete expense nature: %v", err)
		}
		selectedCols = append(selectedCols, "Natureza de Despesa Completa")
	}

	result = result.Select(selectedCols)
	if result.Error() != nil {
		return dataframe.DataFrame{}, fmt.Errorf("error selecting columns: %v", result.Error())
	}

	return result, nil
}

func findRows(df dataframe.DataFrame, dfType DataType, codes []string, codeColumn string, date string, ch chan ExtractedDataframe, wg *sync.WaitGroup) {
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
		ch <- ExtractedDataframe{Dataframe: matchingRows, Type: dfType}
	}
}

func FindCommitmentItemsHistory(commitments []string) []CommitmentItems {
	return []CommitmentItems{}
}

func OpenFileAndDecode(path string) (dataframe.DataFrame, error) {
	const component = "FileDecoder"

	appLogger.Debug(component, "Opening file: path=%s", path)

	file, err := os.Open(path)
	if err != nil {
		appLogger.Error(component, "Failed to open file: path=%s error=%v", path, err)
		return dataframe.DataFrame{}, fmt.Errorf("failed to open file %s: %v", path, err)
	}

	defer file.Close()

	// Using Windows1252 because it is the encoding used by the original CSV files
	decoded := charmap.Windows1252.NewDecoder().Reader(file)
	df := dataframe.ReadCSV(decoded, dataframe.WithDelimiter(';'), dataframe.WithLazyQuotes(true))
	// If dataframe is empty return
	if df.Nrow() == 0 {
		appLogger.Warn(component, "Empty dataframe loaded: path=%s", path)
		return dataframe.DataFrame{}, fmt.Errorf("dataframe is empty")
	}

	appLogger.Debug(component, "File decoded successfully: path=%s rows=%d cols=%d", path, df.Nrow(), df.Ncol())
	return df, df.Error()
}

func filterExtractionsByColumn(extractions []DayExtraction, dataTypes []DataType, codes []string, matchColumn string, chToRelease chan ExtractedDataframe, wg *sync.WaitGroup) []DayExtraction {
	const component = "ExtractionFilter"

	dataTypeNames := make([]string, len(dataTypes))
	for i, dt := range dataTypes {
		dataTypeNames[i] = DataTypeNames[dt]
	}
	appLogger.Info(component, "Starting extraction filter: column=%s dataTypes=%v extractionsCount=%d", matchColumn, dataTypeNames, len(extractions))

	processedFiles := 0
	for _, e := range extractions {
		for _, dt := range dataTypes {
			if p, ok := e.Files[dt]; ok {
				appLogger.Debug(component, "Processing file: date=%s type=%s path=%s", e.Date, DataTypeNames[dt], p)
				df, err := OpenFileAndDecode(p)
				if err != nil {
					appLogger.Warn(component, "Failed to open/decode file: date=%s type=%s path=%s error=%v", e.Date, DataTypeNames[dt], p, err)
					continue
				}
				wg.Add(1)
				go findRows(df, dt, codes, matchColumn, e.Date, chToRelease, wg)
				processedFiles++

				//Remove the actual index to not process it again
				delete(e.Files, dt)
			}
		}
	}

	appLogger.Info(component, "Extraction filter completed: processedFiles=%d", processedFiles)
	return extractions
}

func dfRowToCommitment(df dataframe.DataFrame, rowIdx int) Commitment {
	getStr := func(col string) string {
		if idx := df.Names(); containsString(idx, col) {
			return df.Col(col).Elem(rowIdx).String()
		}
		return ""
	}

	return Commitment{
		CommitmentCode:        getStr("Código Empenho"),
		ResumedCommitmentCode: getStr("Código Empenho Resumido"),
		EmitionDate:           getStr("Data Emissão"),
		Type:                  getStr("Tipo Empenho"),
		ManagementCode:        getStr("Código Gestão"),
		ManagementName:        getStr("Gestão"),
		FavoredCode:           getStr("Código Favorecido"),
		ExpenseNature:         getStr("Elemento de Despesa"),
		CompleteExpenseNature: getStr("Natureza de Despesa Completa"),
		BudgetPlan:            getStr("Plano Orçamentário"),
		Items:                 []CommitmentItem{},
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

	return CommitmentItem{
		ExpenseCategory:    getStr("Categoria de Despesa"),
		ExpenseGroup:       getStr("Grupo de Despesa"),
		AplicationModality: getStr("Modalidade de Aplicação"),
		ExpenseElement:     getStr("Elemento de Despesa"),
		Description:        getStr("Descrição"),
		Sequential:         0,
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

	return CommitmentItemHistory{
		OperationType: getStr("Tipo Operação"),
		OperationDate: getStr("Data Operação"),
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

func ExtractData(extractions []DayExtraction, unitsCode []string) (*CommitmentPayload, error) {
	const component = "DataExtractor"
	var wg sync.WaitGroup
	extractionDate := extractions[0].Date

	appLogger.Info(component, "Starting data extraction: date=%s unitsCount=%d", extractionDate, len(unitsCode))

	mainMatches := make(chan ExtractedDataframe, 3)
	commitmentMatches := make(chan ExtractedDataframe, 3)

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
	extractions = filterExtractionsByColumn(extractions, hasUgCodeAsColumn, unitsCode, "Código Unidade Gestora", mainMatches, &wg)
	wg.Wait()
	close(mainMatches)

	// Collect DataFrames by type
	empenhosDf := dataframe.New()
	liquidacoesDf := dataframe.New()
	pagamentosDf := dataframe.New()

	for extracted := range mainMatches {
		transformedDf, err := transformExpenses(extracted.Dataframe, extracted.Type)
		if err != nil {
			appLogger.Error(component, "DataFrame transformation error: date=%s type=%s error=%v", extractionDate, DataTypeNames[extracted.Type], err)
			continue
		}

		appLogger.Debug(component, "DataFrame transformed: date=%s type=%s rows=%d", extractionDate, DataTypeNames[extracted.Type], transformedDf.Nrow())

		switch extracted.Type {
		case DespesasEmpenho:
			if empenhosDf.Nrow() == 0 {
				empenhosDf = transformedDf
			} else {
				empenhosDf = empenhosDf.Concat(transformedDf)
			}
		case DespesasLiquidacao:
			if liquidacoesDf.Nrow() == 0 {
				liquidacoesDf = transformedDf
			} else {
				liquidacoesDf = liquidacoesDf.Concat(transformedDf)
			}
		case DespesasPagamento:
			if pagamentosDf.Nrow() == 0 {
				pagamentosDf = transformedDf
			} else {
				pagamentosDf = pagamentosDf.Concat(transformedDf)
			}
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
		extractions = filterExtractionsByColumn(extractions, hasCommitmentCodeAsColumn,
			ugsCommitments, "Código Empenho", commitmentMatches, &wg)
		wg.Wait()
		close(commitmentMatches)

		for extracted := range commitmentMatches {
			transformedDf, err := transformExpenses(extracted.Dataframe, extracted.Type)
			if err != nil {
				appLogger.Error(component, "Commitment items transformation error: date=%s type=%s error=%v", extractionDate, DataTypeNames[extracted.Type], err)
				continue
			}

			switch extracted.Type {
			case DespesasItemEmpenho:
				if itemsDf.Nrow() == 0 {
					itemsDf = transformedDf
				} else {
					itemsDf = itemsDf.Concat(transformedDf)
				}
			case DespesasItemEmpenhoHistorico:
				if historyDf.Nrow() == 0 {
					historyDf = transformedDf
				} else {
					historyDf = historyDf.Concat(transformedDf)
				}
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
		ExtractionDate:  extractionDate,
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

func main() {
	const component = "Main"
	monitor := NewMonitor()
	monitor.Start(100 * time.Millisecond)

	// Configure log output format
	log.SetFlags(0) // Remove default timestamp since we add our own

	var url string
	yesterday := time.Now().AddDate(0, 0, -1).Format("2006-01-02")
	initDatePtr := flag.String("init", yesterday, "Initial date for data extraction")
	endDatePtr := flag.String("end", yesterday, "End date for data extraction")
	ugsPtr := flag.String("ugs", "158454,158148,158341,158342,158343,158345,158376,158332,158533,158635,158636", "Comma-separated list of Unit Codes to extract")
	logLevelPtr := flag.String("loglevel", "info", "Log level: debug, info, warn, error")
	flag.Parse()

	// Set log level based on flag
	switch strings.ToLower(*logLevelPtr) {
	case "debug":
		appLogger.SetLogLevel(LevelDebug)
	case "info":
		appLogger.SetLogLevel(LevelInfo)
	case "warn":
		appLogger.SetLogLevel(LevelWarn)
	case "error":
		appLogger.SetLogLevel(LevelError)
	default:
		appLogger.SetLogLevel(LevelInfo)
	}

	init_date := *initDatePtr
	end_date := *endDatePtr
	ugs := strings.Split(*ugsPtr, ",")

	appLogger.Info(component, "Application started: initDate=%s endDate=%s ugsCount=%d logLevel=%s", init_date, end_date, len(ugs), *logLevelPtr)

	// Create necessary directories
	dirs := []string{"tmp/zips", "tmp/data"}
	MAX_CONCURRENT_EXTRACTIONS_DATA := 7
	extractions_semaphore := make(chan struct{}, MAX_CONCURRENT_EXTRACTIONS_DATA)

	appLogger.Debug(component, "Creating required directories: dirs=%v", dirs)
	for _, dir := range dirs {
		err := createDirsIfNotExist(dir)
		if err != nil {
			appLogger.Fatal(component, "Failed to create directory: dir=%s error=%v", dir, err)
		}
	}

	init_parsed_date, err := time.Parse(time.DateOnly, init_date)
	if err != nil {
		appLogger.Fatal(component, "Invalid init date format: date=%s error=%v", init_date, err)
	}
	end_parsed_date, err := time.Parse(time.DateOnly, end_date)
	if err != nil {
		appLogger.Fatal(component, "Invalid end date format: date=%s error=%v", end_date, err)
	}

	var extractions []DayExtraction

	var mu sync.Mutex

	var wg sync.WaitGroup

	appLogger.Info(component, "Starting download phase")
	downloadCount := 0
	for !init_parsed_date.After(end_parsed_date) {
		init_date = init_parsed_date.Format("20060102")
		url = PORTAL_TRANSPARENCIA_URL + init_date
		wg.Add(1)
		downloadCount++
		go func(u, d string) {
			defer wg.Done()
			download := fetchData(u, d)
			var extraction ExtractionResult

			if !download.Success {
				appLogger.Warn(component, "Download failed: date=%s", d)
				return
			}

			if download.OutputPath != "" {
				extraction = unzipFile(download.OutputPath, "tmp/data/despesas_"+d)
			}

			if !extraction.Success {
				appLogger.Warn(component, "Extraction failed: date=%s", d)
				return
			}

			files := buildFilesForDate(d, extraction.OutputDir)

			mu.Lock()
			extractions = append(extractions, DayExtraction{Date: d, Files: files})
			mu.Unlock()
			appLogger.Info(component, "Day extraction ready: date=%s outputDir=%s", d, extraction.OutputDir)

		}(url, init_date)
		init_parsed_date = init_parsed_date.AddDate(0, 0, 1)
	}
	appLogger.Info(component, "Waiting for downloads to complete: totalDays=%d", downloadCount)
	wg.Wait()

	// Create a goroutine for each day
	createDirsIfNotExist("output")
	appLogger.Info(component, "Starting data processing phase: extractionsReady=%d maxConcurrent=%d", len(extractions), MAX_CONCURRENT_EXTRACTIONS_DATA)

	for _, extraction := range extractions {
		wg.Add(1)
		go func(ex DayExtraction) {
			defer wg.Done()
			extractions_semaphore <- struct{}{}
			defer func() { <-extractions_semaphore }()

			appLogger.Debug(component, "Processing extraction: date=%s", ex.Date)

			//Memory intensive
			payload, err := ExtractData([]DayExtraction{ex}, ugs)
			if err != nil {
				appLogger.Warn(component, "Data extraction skipped: date=%s reason=%v", ex.Date, err)
				return
			}

			jsonData, err := json.Marshal(payload)
			if err != nil {
				appLogger.Error(component, "JSON marshaling failed: date=%s error=%v", ex.Date, err)
				return
			}

			outputPath := fmt.Sprintf("output/extraction_%s.json", ex.Date)
			if err := os.WriteFile(outputPath, jsonData, 0644); err != nil {
				appLogger.Error(component, "Output file write failed: date=%s path=%s error=%v", ex.Date, outputPath, err)
			} else {
				appLogger.Info(component, "Output file written: date=%s path=%s size=%d bytes", ex.Date, outputPath, len(jsonData))
			}

		}(extraction)
	}
	clearTempDirs()

	wg.Wait()
	appLogger.Info(component, "Application completed successfully")
}
