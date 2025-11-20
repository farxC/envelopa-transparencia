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
	"golang.org/x/text/encoding/charmap"
)

type DataType int

const (
	DespesasEmpenho DataType = iota
	DespesasItemEmpenho
	DespesasLiquidacao
	DespesasPagamento
	DespesasPagamentoEmpenhosImpactados
	DespesasPagamentoFavoricidosFinais
	DespesasLiquidacaoEmpenhos
	DespesasLiquidacaoEmpenhosImpactados
	DespesasPagamentoListaBancos
)

const (
	DespesasEmpenhoDataType                      = "_Despesas_Empenho.csv"
	DespesasItemEmpenhoDataType                  = "_Despesas_Item_Empenho.csv"
	DespesasLiquidacaoDataType                   = "_Despesas_Liquidacao.csv"
	DespesasPagamentoDataType                    = "_Despesas_Pagamento.csv"
	DespesasLiquidacaoEmpenhosImpactadosDataType = "_Despesas_Liquidacao_EmpenhosImpactados.csv"
	DespesasPagamentoEmpenhosImpactadosDataType  = "_Despesas_Pagamento_EmpenhosImpactados.csv"
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

var dataTypeSuffix = map[DataType]string{
	DespesasEmpenho:                      DespesasEmpenhoDataType,
	DespesasItemEmpenho:                  DespesasItemEmpenhoDataType,
	DespesasLiquidacao:                   DespesasLiquidacaoDataType,
	DespesasPagamento:                    DespesasPagamentoDataType,
	DespesasLiquidacaoEmpenhosImpactados: DespesasLiquidacaoEmpenhosImpactadosDataType,
	DespesasPagamentoEmpenhosImpactados:  DespesasPagamentoEmpenhosImpactadosDataType,
}

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

	fmt.Printf("Data successfully downloaded to %s\n", output_path)

	return DownloadResult{Success: true, OutputPath: output_path}
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

func BulkExtractCommitments(extractions []DayExtraction, unitCode []string) {
	for _, e := range extractions {
		if p, ok := e.Files[DespesasEmpenho]; ok {
			file, err := os.Open(p)

			if err != nil {
				log.Fatalf("Failed to open file %s: %v", p, err)
			}

			func() {
				defer file.Close()

				decoded := charmap.Windows1252.NewDecoder().Reader(file)

				df := dataframe.ReadCSV(decoded, dataframe.WithDelimiter(';'), dataframe.WithLazyQuotes(true))

				fmt.Println(df)

			}()

		}

	}
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
	var mu sync.Mutex

	var wg sync.WaitGroup

	for !init_parsed_date.After(end_parsed_date) {
		init_date = init_parsed_date.Format("20060102")
		url = "https://portaldatransparencia.gov.br/download-de-dados/despesas/" + init_date
		wg.Add(1)
		go func(u, d string) {
			defer wg.Done()
			download := fetchData(u, d)

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

	BulkExtractCommitments(extractions, []string{""})
}
