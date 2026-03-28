package service

type DownloadResult struct {
	Success    bool
	OutputPath string
}

type TransparencyPortalClient interface {
	FetchExpensesData(date string) DownloadResult
	ExtractExpenses(cfg ExpensesExtractionConfig) (*ExpensesPayload, error)
	FetchExpensesExecution(month, year string) DownloadResult
	ExtractExpensesExecution(cfg ExpensesExecutionExtractionConfig) (*[]UnitExpenseExecution, error)
}
