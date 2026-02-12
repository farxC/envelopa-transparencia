package downloader

import (
	"io"
	"net/http"
	"os"

	"github.com/farxc/envelopa-transparencia/internal/logger"
)

var PortalTransparenciaURL = "https://portaldatransparencia.gov.br/download-de-dados/despesas/"

type DownloadResult struct {
	Success    bool
	OutputPath string
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
