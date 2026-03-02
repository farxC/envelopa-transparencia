package filesystem

import (
	"archive/zip"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/farxc/envelopa-transparencia/internal/domain/service"
	"github.com/farxc/envelopa-transparencia/internal/infrastructure/logger"
	"github.com/go-gota/gota/dataframe"
	"golang.org/x/text/encoding/charmap"
)

type ExtractionResult struct {
	Success   bool
	Data      service.DataType
	OutputDir string
}

var dataserviceuffix = map[service.DataType]string{
	service.DespesasEmpenho:                      service.DespesasEmpenhoDataType,
	service.DespesasItemEmpenho:                  service.DespesasItemEmpenhoDataType,
	service.DespesasItemEmpenhoHistorico:         service.DespesasItemEmpenhoHistoricoDataType,
	service.DespesasLiquidacao:                   service.DespesasLiquidacaoDataType,
	service.DespesasPagamento:                    service.DespesasPagamentoDataType,
	service.DespesasLiquidacaoEmpenhosImpactados: service.DespesasLiquidacaoEmpenhosImpactadosDataType,
	service.DespesasPagamentoEmpenhosImpactados:  service.DespesasPagamentoEmpenhosImpactadosDataType,
	service.DespesasPagamentoListaBancos:         service.DespesasPagamentoListaBancosDataType,
	service.DespesasPagamentoListaFaturas:        service.DespesasPagamentoListaFaturasDataType,
	service.DespesasPagamentoListaPrecatorios:    service.DespesasPagamentoListaPrecatoriosDataType,
}

var notUsedFiles = []service.DataType{
	service.DespesasPagamentoListaBancos,
	service.DespesasPagamentoListaFaturas,
	service.DespesasPagamentoListaPrecatorios,
}

func isFileUsed(filename string) bool {
	for _, v := range notUsedFiles {
		if strings.HasSuffix(filename, dataserviceuffix[v]) {
			return false
		}
	}

	return true
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
	return ExtractionResult{Success: true, Data: service.DespesasEmpenho, OutputDir: destDir}
}

func BuildFilesForDate(date, dir string) map[service.DataType]string {
	m := make(map[service.DataType]string, len(dataserviceuffix))

	for dt, suffix := range dataserviceuffix {
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
