package application

import (
	"archive/zip"
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/farxc/envelopa-transparencia/internal/domain/model"
	"github.com/farxc/envelopa-transparencia/internal/domain/service"
	"github.com/farxc/envelopa-transparencia/internal/infrastructure/logger"
)

// fakeBudgetClient serves a fresh budget ZIP on FetchBudget and records what
// ExtractBudget was given. Methods of other kinds are not used.
type fakeBudgetClient struct {
	service.TransparencyPortalClient
	fetchOK   bool
	fetches   int
	extracted string // contents of the CSV ExtractBudget read
}

func (f *fakeBudgetClient) FetchBudget(year string) service.DownloadResult {
	f.fetches++
	if !f.fetchOK {
		return service.DownloadResult{Success: false}
	}
	path := "tmp/zips/budget/" + year + "_OrcamentoDespesa.zip"
	writeZip(path, year+service.OrcamentoDespesaCSVSuffix, "fresh")
	return service.DownloadResult{Success: true, OutputPath: path}
}

func (f *fakeBudgetClient) ExtractBudget(cfg service.BudgetExtractionConfig) (*service.BudgetPayload, error) {
	b, err := os.ReadFile(cfg.File)
	if err != nil {
		return nil, err
	}
	f.extracted = string(b)
	return &service.BudgetPayload{Year: cfg.Year}, nil
}

type nopLoader struct{ service.Loader }

func (nopLoader) LoadExpenseBudget(context.Context, *service.BudgetPayload) error { return nil }

func writeZip(path, name, content string) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		panic(err)
	}
	f, err := os.Create(path)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	zw := zip.NewWriter(f)
	w, err := zw.Create(name)
	if err != nil {
		panic(err)
	}
	w.Write([]byte(content))
	if err := zw.Close(); err != nil {
		panic(err)
	}
}

func TestBudgetPipeline_IgnoresCachedZip(t *testing.T) {
	t.Chdir(t.TempDir())
	// A ZIP from an earlier run: the portal regenerates the file daily, so it
	// must not be reused.
	writeZip("tmp/zips/budget/2026_OrcamentoDespesa.zip", "2026"+service.OrcamentoDespesaCSVSuffix, "stale")

	client := &fakeBudgetClient{fetchOK: true}
	p := NewBudgetPipeline(client, nopLoader{}, &logger.Logger{MinLevel: logger.LevelError})

	if err := p.Execute(context.Background(), model.BudgetJob{Year: "2026", Codes: []int64{26421}}); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if client.fetches != 1 {
		t.Errorf("FetchBudget called %d times, want 1", client.fetches)
	}
	if client.extracted != "fresh" {
		t.Errorf("extracted %q, want the freshly downloaded file", client.extracted)
	}
}

func TestBudgetPipeline_FailedDownloadDoesNotUseCache(t *testing.T) {
	t.Chdir(t.TempDir())
	writeZip("tmp/zips/budget/2026_OrcamentoDespesa.zip", "2026"+service.OrcamentoDespesaCSVSuffix, "stale")

	client := &fakeBudgetClient{fetchOK: false}
	p := NewBudgetPipeline(client, nopLoader{}, &logger.Logger{MinLevel: logger.LevelError})

	if err := p.Execute(context.Background(), model.BudgetJob{Year: "2026", Codes: []int64{26421}}); err == nil {
		t.Fatal("expected an error when the download fails")
	}
	if client.extracted != "" {
		t.Errorf("extracted %q from the stale cache, want nothing extracted", client.extracted)
	}
}
