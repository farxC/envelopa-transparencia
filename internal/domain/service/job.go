package service

type ExtractionConfig[T any] struct {
	Codes          []string
	IsManagingCode bool
	Extraction     T
}

type ExpensesExtractionConfig = ExtractionConfig[OutputExpensesExtractionFiles]

type ExpensesExecutionExtractionConfig = ExtractionConfig[OutputExpensesExecutionExtractionFiles]
