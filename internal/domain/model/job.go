package model

import "time"

// ExpensesDailyJob represents a single day of expenses lifecycle data to ingest.
// Granularity: daily (one job per date).
type ExpensesDailyJob struct {
	Date           time.Time
	Codes          []int64
	IsManagingCode bool
	Trigger        string
}

// ExpensesExecutionJob represents a month of aggregated execution data to ingest.
// Granularity: monthly (one job per year+month pair).
type ExpensesExecutionJob struct {
	Year           string // "2025"
	Month          string // "01"
	Codes          []int64
	IsManagingCode bool
	Trigger        string
}
