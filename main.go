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
	DespesasItemEmpenhoDataType                  = "_Despesas_Item_Empenho.csv"
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

var dataTypeSuffix = map[DataType]string{
	DespesasEmpenho:                      DespesasEmpenhoDataType,
	DespesasItemEmpenho:                  DespesasItemEmpenhoDataType,
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
}

func transformExpenses(df dataframe.DataFrame, dfType DataType) (dataframe.DataFrame, error) {
	selectedCols, ok := columnsForDataType[dfType]
	if !ok {
		return dataframe.DataFrame{}, fmt.Errorf("unsupported data type for transformation: %v", dfType)
	}
	result, err := concatCompleteExpenseNature(df)

	if err != nil {
		return dataframe.DataFrame{}, fmt.Errorf("error concatenating complete expense nature: %v", err)
	}
	selectedCols = append(selectedCols, "Natureza de Despesa Completa")
	result = result.Select(selectedCols)
	if result.Error() != nil {
		return dataframe.DataFrame{}, fmt.Errorf("error selecting columns: %v", result.Error())
	}

	return result, nil
}

func findUnitsRows(df dataframe.DataFrame, dfType DataType, ugCodes []string, matchChan chan ExtractedDataframe, wg *sync.WaitGroup) {
	defer wg.Done()
	log.Printf("Searching for units codes: %v", ugCodes)
	filter := dataframe.F{
		Colname:    "Código Unidade Gestora",
		Comparator: series.In,
		Comparando: ugCodes,
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
		matchChan <- ExtractedDataframe{Dataframe: matchingRows, Type: dfType}
	}

}

func ExtractData(extractions []DayExtraction, unitsCode []string) {
	var wg sync.WaitGroup

	mainMatches := make(chan ExtractedDataframe, 3)

	hasUgCodeAsColumn := []DataType{
		DespesasEmpenho,
		DespesasLiquidacao,
		DespesasPagamento,
	}
	for _, e := range extractions {
		for _, dt := range hasUgCodeAsColumn {
			if p, ok := e.Files[dt]; ok {
				fmt.Printf("Processing file for date %s and data type %s\n", e.Date, dt)
				file, err := os.Open(p)

				if err != nil {
					log.Printf("Failed to open file %s: %v", p, err)
					return
				}

				decoded := charmap.Windows1252.NewDecoder().Reader(file)

				df := dataframe.ReadCSV(decoded, dataframe.WithDelimiter(';'), dataframe.WithLazyQuotes(true))

				file.Close()

				wg.Add(1)
				go findUnitsRows(df, dt, unitsCode, mainMatches, &wg)
			}
		}
	}
	wg.Wait()

	close(mainMatches)
	var res = dataframe.New()
	fmt.Printf("Found %d matching DataFrames\n", len(mainMatches))
	transformed_dfs := make([]dataframe.DataFrame, 0)

	// First search for matching rows in the main information data (required for sub-extractions)
	for extracted := range mainMatches {
		fmt.Println("Before transformation:", extracted.Dataframe.Nrow(), "rows and", extracted.Dataframe.Ncol(), "columns")
		transformedDf, err := transformExpenses(extracted.Dataframe, extracted.Type)
		fmt.Println("After transformation:", transformedDf.Nrow(), "rows and", transformedDf.Ncol(), "columns")
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
	fmt.Println(ugsCommitments)

	// TO DO: Create logic for sub-extractions (items, items history, liquidations impacted, payments impacted)
	saveDataFrame(res, "combined_data.csv")
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
				DespesasEmpenho:     "tmp/data/despesas_20250101/20250101_Despesas_Empenho.csv",
				DespesasItemEmpenho: "tmp/data/despesas_20250101/20250101_Despesas_Item_Empenho.csv",
				DespesasLiquidacao:  "tmp/data/despesas_20250101/20250101_Despesas_Liquidacao.csv",
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
