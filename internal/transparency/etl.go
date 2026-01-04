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

type ExtractedDataframe struct {
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

func OpenFileAndDecode(path string, appLogger *logger.Logger) (dataframe.DataFrame, error) {
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

func findRows(df dataframe.DataFrame, dfType DataType, codes []string, codeColumn string, date string, ch chan ExtractedDataframe, wg *sync.WaitGroup, appLogger *logger.Logger) {
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

func filterExtractionsByColumn(extractions []DayExtraction, dataTypes []DataType, codes []string, matchColumn string, chToRelease chan ExtractedDataframe, wg *sync.WaitGroup, appLogger *logger.Logger) []DayExtraction {
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
				df, err := OpenFileAndDecode(p, appLogger)
				if err != nil {
					appLogger.Warn(component, "Failed to open/decode file: date=%s type=%s path=%s error=%v", e.Date, DataTypeNames[dt], p, err)
					continue
				}
				wg.Add(1)
				go findRows(df, dt, codes, matchColumn, e.Date, chToRelease, wg, appLogger)
				processedFiles++

				//Remove the actual index to not process it again
				delete(e.Files, dt)
			}
		}
	}

	appLogger.Info(component, "Extraction filter completed: processedFiles=%d", processedFiles)
	return extractions
}

func ExtractData(extractions []DayExtraction, unitsCode []string, appLogger *logger.Logger) (*CommitmentPayload, error) {
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
	extractions = filterExtractionsByColumn(extractions, hasUgCodeAsColumn, unitsCode, "Código Unidade Gestora", mainMatches, &wg, appLogger)
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
			ugsCommitments, "Código Empenho", commitmentMatches, &wg, appLogger)
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
