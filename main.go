package main

import (
	"archive/zip"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/go-gota/gota/dataframe"
	"github.com/go-gota/gota/series"
	"golang.org/x/text/encoding/charmap"
)

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

func fetchData(downloadUrl string, date string) DownloadResult {
	output_path := "tmp/zips/despesas_" + date + ".zip"

	client := &http.Client{}

	client.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		req.Header.Add("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3")
		return nil
	}
	// Create a new request with a custom User-Agent header
	req, err := http.NewRequest(http.MethodGet, downloadUrl, nil)
	if err != nil {
		log.Fatalf("Failed to create request: %v", err)
		return DownloadResult{Success: false}
	}

	resp, err := client.Do(req)

	if err != nil {
		log.Fatalf("Failed to fetch data: %v", err)
		return DownloadResult{Success: false}
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		log.Fatalf("Failed to fetch data: %s", resp.Status)
		return DownloadResult{Success: false}
	}

	out, err := os.Create(output_path)

	if err != nil {
		log.Fatalf("Failed to create file: %v", err)
		return DownloadResult{Success: false}
	}
	defer out.Close()

	_, err = io.Copy(out, resp.Body)
	if err != nil {
		log.Fatalf("Failed to write data to file: %v", err)
		return DownloadResult{Success: false}
	}

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

	if destDir == "" {
		destDir = "tmp/data"
	}

	err := os.MkdirAll(destDir, os.ModePerm)
	if err != nil {
		log.Fatalf("Failed to create directory: %v", err)
		return ExtractionResult{Success: false}
	}

	r, err := zip.OpenReader(zipPath)
	if err != nil {
		log.Fatalf("Failed to open zip file: %v", err)
		return ExtractionResult{Success: false}
	}
	defer r.Close()

	for _, f := range r.File {
		filePath := filepath.Join(destDir, f.Name)

		if !strings.HasPrefix(filePath, filepath.Clean(destDir)+string(os.PathSeparator)) {
			return ExtractionResult{Success: false}
		}

		if !isFileUsed(f.Name) {
			continue
		}

		destFile, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, f.Mode())
		if err != nil {
			return ExtractionResult{Success: false}
		}
		defer destFile.Close()

		zippedFile, err := f.Open()
		if err != nil {
			return ExtractionResult{Success: false}
		}
		defer zippedFile.Close()

		if _, err := io.Copy(destFile, zippedFile); err != nil {
			return ExtractionResult{Success: false}
		}

	}
	return ExtractionResult{Success: true, Data: DespesasEmpenho, OutputDir: destDir}
}

func buildFilesForDate(date, dir string) map[DataType]string {
	m := make(map[DataType]string, len(dataTypeSuffix))

	for dt, suffix := range dataTypeSuffix {
		m[dt] = filepath.Join(dir, date+suffix)
	}
	return m
}

func saveDataFrame(df dataframe.DataFrame, filename string) error {
	f, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("error creating file: %v", err)
	}
	defer f.Close()

	if err := df.WriteCSV(f); err != nil {
		return fmt.Errorf("error writing CSV: %v", err)
	}

	return nil
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
	defer wg.Done()
	log.Printf("Searching for %s codes: %v", codeColumn, codes)

	filter := dataframe.F{
		Colname:    codeColumn,
		Comparator: series.In,
		Comparando: codes,
	}

	matchingRows := df.Filter(
		filter,
	)

	if df.Error() != nil {
		log.Printf("Error filtering DataFrame: %v", df.Error())
		return
	}

	fmt.Printf("%s Found rows: %d\n for type: %s\n", date, matchingRows.Nrow(), DataTypeNames[dfType])
	if matchingRows.Nrow() > 0 {
		ch <- ExtractedDataframe{Dataframe: matchingRows, Type: dfType}
	}

}

// Future improvements
func removeExtraction(sl []DayExtraction, idx int) []DayExtraction {
	if idx < 0 || idx >= len(sl) {
		return sl
	}
	return append(sl[:idx], sl[idx+1:]...)
}

func FindCommitmentItemsHistory(commitments []string) []CommitmentItems {
	return []CommitmentItems{}
}

func OpenFileAndDecode(path string) (dataframe.DataFrame, error) {
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

func filterExtractionsByColumn(extractions []DayExtraction, dataTypes []DataType, codes []string, matchColumn string, chToRelease chan ExtractedDataframe, wg *sync.WaitGroup) []DayExtraction {
	fmt.Printf("Filtering extractions by column and types: %s %v\n", matchColumn, dataTypes)
	for _, e := range extractions {
		for _, dt := range dataTypes {
			if p, ok := e.Files[dt]; ok {
				df, err := OpenFileAndDecode(p)
				if err != nil {
					log.Printf("Failed to open and decode file %s: %v", p, err)
					continue
				}
				wg.Add(1)
				go findRows(df, dt, codes, matchColumn, e.Date, chToRelease, wg)

				//Remove the actual index to not process it again
				delete(e.Files, dt)
			}
		}
	}
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
		Sequential:         0, // Parse from string if needed
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
	var wg sync.WaitGroup
	extractionDate := extractions[0].Date

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
			log.Printf("Error transforming DataFrame for type %v: %v", DataTypeNames[extracted.Type], err)
			continue
		}

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
	if !hasAnyData {
		return nil, fmt.Errorf("no matching data found for extraction date %s", extractionDate)
	}

	// Only extract commitment items if we have commitments
	itemsDf := dataframe.DataFrame{}
	historyDf := dataframe.DataFrame{}

	if empenhosDf.Nrow() > 0 {
		// Get commitment codes for sub-extraction
		ugsCommitments := empenhosDf.Col("Código Empenho").Records()

		// Extract commitment items and history
		extractions = filterExtractionsByColumn(extractions, hasCommitmentCodeAsColumn,
			ugsCommitments, "Código Empenho", commitmentMatches, &wg)
		wg.Wait()
		close(commitmentMatches)

		for extracted := range commitmentMatches {
			transformedDf, err := transformExpenses(extracted.Dataframe, extracted.Type)
			if err != nil {
				log.Printf("Error transforming DataFrame for type %v: %v", extracted.Type, err)
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
	} else {
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

	return payload, nil
}

func main() {
	var url string
	init_date := "2025-01-16"
	end_date := "2025-02-16"
	dirs := []string{"tmp/zips", "tmp/data"}
	MAX_CONCURRENT_EXTRACTIONS_DATA := 7
	extractions_semaphore := make(chan struct{}, MAX_CONCURRENT_EXTRACTIONS_DATA)

	for _, dir := range dirs {
		err := createDirsIfNotExist(dir)
		if err != nil {
			log.Fatalf("Failed to create directory %s: %v", dir, err)
		}
	}

	init_parsed_date, err := time.Parse(time.DateOnly, init_date)
	if err != nil {
		log.Fatal(err)
	}
	end_parsed_date, err := time.Parse(time.DateOnly, end_date)
	if err != nil {
		log.Fatal(err)
	}

	var extractions []DayExtraction
	// mockedExtractions := []DayExtraction{
	// 	{
	// 		Date: "20250101",
	// 		Files: map[DataType]string{
	// 			DespesasEmpenho:              "tmp/data/despesas_20250101/20250101_Despesas_Empenho.csv",
	// 			DespesasItemEmpenho:          "tmp/data/despesas_20250101/20250101_Despesas_ItemEmpenho.csv",
	// 			DespesasLiquidacao:           "tmp/data/despesas_20250101/20250101_Despesas_Liquidacao.csv",
	// 			DespesasItemEmpenhoHistorico: "tmp/data/despesas_20250101/20250101_Despesas_ItemEmpenhoHistorico.csv",
	// 		},
	// 	},
	// }

	var mu sync.Mutex

	var wg sync.WaitGroup

	for !init_parsed_date.After(end_parsed_date) {
		init_date = init_parsed_date.Format("20060102")
		url = PORTAL_TRANSPARENCIA_URL + init_date
		wg.Add(1)
		go func(u, d string) {
			defer wg.Done()
			download := fetchData(u, d)
			// mockedDownload := DownloadResult{
			// 	Success:    true,
			// 	OutputPath: "tmp/zips/despesas_" + d + ".zip",
			// }

			if !download.Success {
				log.Printf("Download failed for date %s\n", d)
				return
			}

			extraction := unzipFile(download.OutputPath, "tmp/data/despesas_"+d)

			if !extraction.Success {
				log.Printf("Extraction failed for date %s\n", d)
				return
			}

			files := buildFilesForDate(d, extraction.OutputDir)

			mu.Lock()
			extractions = append(extractions, DayExtraction{Date: d, Files: files})
			mu.Unlock()
			fmt.Printf("Data for date %s successfully extracted to %s\n", d, extraction.OutputDir)

		}(url, init_date)
		init_parsed_date = init_parsed_date.AddDate(0, 0, 1)
	}
	wg.Wait()
	// BulkExtractCommitments(mockedExtractions, []string{"158454"})
	// Create a goroutine for each day
	createDirsIfNotExist("output")
	for _, extraction := range extractions {
		wg.Add(1)
		go func(ex DayExtraction) {
			defer wg.Done()
			extractions_semaphore <- struct{}{}
			defer func() { <-extractions_semaphore }()

			//Memory intensive
			payload, err := ExtractData([]DayExtraction{ex}, []string{"158454", "158148", "158341", "158342", "158343", "158345", "158376", "158332", "158533", "158635", "158636"})
			if err != nil {
				log.Printf("Error extracting data for date and type %s: %v\n", ex.Date, err)
				return
			}
			jsonData, err := json.Marshal(payload)
			if err != nil {
				log.Printf("Error marshaling JSON for date %s: %v\n", ex.Date, err)
				<-extractions_semaphore
				return
			}
			outputPath := fmt.Sprintf("output/extraction_%s.json", ex.Date)
			if err := os.WriteFile(outputPath, jsonData, 0644); err != nil {
				log.Printf("Error writing output file for date %s: %v\n", ex.Date, err)
			} else {
				log.Printf("Output file for date %s written successfully\n", ex.Date)
			}

		}(extraction)
	}

	wg.Wait()
}
