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

type DownloadResult struct {
	Success    bool
	OutputPath string
}

type ExtractionResult struct {
	Success   bool
	Data      DataType
	OutputDir string
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

func main() {
	url := "https://portaldatransparencia.gov.br/download-de-dados/despesas/20250101"
	init_date := "2023-01-01"
	end_date := "2023-04-30"
	init_parsed_date, err := time.Parse(time.DateOnly, init_date)
	if err != nil {
		log.Fatal(err)
	}
	end_parsed_date, err := time.Parse(time.DateOnly, end_date)
	if err != nil {
		log.Fatal(err)
	}

	var wg sync.WaitGroup

	for !init_parsed_date.After(end_parsed_date) {
		init_date = init_parsed_date.Format("20060102")
		url = "https://portaldatransparencia.gov.br/download-de-dados/despesas/" + init_date
		wg.Add(1)
		go func(u, d string) {
			defer wg.Done()
			output_path := fetchData(u, d)
			if output_path.Success {
				extraction_path := unzipFile(output_path.OutputPath, "tmp/data/despesas_"+d)
				if extraction_path.Success {
					fmt.Printf("Data successfully extracted to %s\n", extraction_path.OutputDir)
				} else {
					fmt.Println("Failed to extract data")
				}
			}
		}(url, init_date)
		init_parsed_date = init_parsed_date.AddDate(0, 1, 0)
	}
	wg.Wait()
}
