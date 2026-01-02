package main

import (
	"archive/zip"
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

var columnsForDataType = map[DataType][]string{
	DespesasPagamento: {
		"Código Pagamento",
		"Código Pagamento Resumido",
		"Data Emissão",
		"Código Tipo Documento",
		"Tipo Documento",
		"Tipo OB",
		"Extraornamentário",
		"Processo",
		"Código Unidade Gestora",
		"Unidade Gestora",
		"Código Gestão",
		"Gestão",
		"Código Favorecido",
		"Favorecido",
		"Valor Original do Pagamento",
		"Valor do Pagamento Convertido para R$",
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

	var result dataframe.DataFrame

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

func findRows(df dataframe.DataFrame, dfType DataType, codes []string, codeColumn string, ch chan ExtractedDataframe, wg *sync.WaitGroup) {
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

	fmt.Printf("Found rows: %d\n", matchingRows.Nrow())
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
				go findRows(df, dt, codes, matchColumn, chToRelease, wg)

				//Remove the actual index to not process it again
				delete(e.Files, dt)
			}
		}
	}
	return extractions
}

func ExtractData(extractions []DayExtraction, unitsCode []string) {
	var wg sync.WaitGroup

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
		// DespesasLiquidacaoEmpenhosImpactados,
		// DespesasPagamentoEmpenhosImpactados,
	}

	//First, find all Commitments based in Unit Codes
	extractions = filterExtractionsByColumn(extractions, hasUgCodeAsColumn, unitsCode, "Código Unidade Gestora", mainMatches, &wg)
	wg.Wait()

	close(mainMatches)
	var res = dataframe.New()
	fmt.Printf("Found %d matching DataFrames\n", len(mainMatches))
	transformed_dfs := make([]dataframe.DataFrame, 0)

	// First search for matching rows in the main information data (required for sub-extractions)
	for extracted := range mainMatches {
		transformedDf, err := transformExpenses(extracted.Dataframe, extracted.Type)
		if err != nil {
			log.Printf("Error transforming DataFrame for type %v: %v", extracted.Type, err)
			continue

		}
		transformed_dfs = append(transformed_dfs, transformedDf)
	}

	if res.Nrow() == 0 {
		res = transformed_dfs[0]
	}
	for i := 1; i < len(transformed_dfs); i++ {
		res = res.Concat(transformed_dfs[i])
	}

	var ugsCommitments []string
	if len(transformed_dfs) > 0 {
		ugsCommitments = transformed_dfs[0].Col("Código Empenho").Records()
	}

	// TO DO: Create logic for sub-extractions (items, items history, liquidations impacted, payments impacted)
	extractions = filterExtractionsByColumn(extractions, hasCommitmentCodeAsColumn,
		ugsCommitments, "Código Empenho", commitmentMatches, &wg)
	wg.Wait()

	close(commitmentMatches)
	fmt.Printf("Commitment matches found: %d\n", len(commitmentMatches))
	commitmentDfs := make([]dataframe.DataFrame, 0)

	var res_2 = dataframe.New()

	for extracted := range commitmentMatches {
		transformedDf, err := transformExpenses(extracted.Dataframe, extracted.Type)
		fmt.Printf("Columns transformed: %v\n", transformedDf.Names())

		if err != nil {
			log.Printf("Error transforming DataFrame for type %v: %v", extracted.Type, err)
			continue
		}
		commitmentDfs = append(commitmentDfs, transformedDf)
	}

	if len(commitmentDfs) > 0 {
		res_2 = commitmentDfs[0]
	}
	for i := 1; i < len(commitmentDfs); i++ {
		res_2 = res_2.Concat(commitmentDfs[i])
	}

	saveDataFrame(res, "combined_data.csv")
	saveDataFrame(res_2, "combined_commitment_data.csv")

	fmt.Printf("Combined DataFrame has %d rows and %d columns\n", res.Nrow(), res.Ncol())

}

func main() {
	var url string
	init_date := "2025-01-01"
	end_date := "2025-01-02"
	dirs := []string{"tmp/zips", "tmp/data"}

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
	mockedExtractions := []DayExtraction{
		DayExtraction{
			Date: "20250101",
			Files: map[DataType]string{
				DespesasEmpenho:              "tmp/data/despesas_20250101/20250101_Despesas_Empenho.csv",
				DespesasItemEmpenho:          "tmp/data/despesas_20250101/20250101_Despesas_ItemEmpenho.csv",
				DespesasLiquidacao:           "tmp/data/despesas_20250101/20250101_Despesas_Liquidacao.csv",
				DespesasItemEmpenhoHistorico: "tmp/data/despesas_20250101/20250101_Despesas_ItemEmpenhoHistorico.csv",
			},
		},
	}

	var mu sync.Mutex

	var wg sync.WaitGroup

	for !init_parsed_date.After(end_parsed_date) {
		init_date = init_parsed_date.Format("20060102")
		url = PORTAL_TRANSPARENCIA_URL + init_date
		wg.Add(1)
		go func(u, d string) {
			defer wg.Done()
			// download := fetchData(u, d)
			mockedDownload := DownloadResult{
				Success:    true,
				OutputPath: "tmp/zips/despesas_" + d + ".zip",
			}

			if !mockedDownload.Success {
				log.Printf("Download failed for date %s\n", d)
				return
			}

			extraction := unzipFile(mockedDownload.OutputPath, "tmp/data/despesas_"+d)

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
	ExtractData(mockedExtractions, []string{"155230"})

}
