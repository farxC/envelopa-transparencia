package service

type DownloadResult struct {
	Success    bool
	OutputPath string
}

type TransparencyPortalClient interface {
	FetchExpensesData(downloadUrl string, date string) DownloadResult
	ExtractExpenses(extraction OutputExtractionFiles, codes []string, isManagingCode bool) (*ExpensesPayload, error)
}
