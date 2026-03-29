package service

import (
	"context"
)

type Loader interface {
	LoadExpenses(ctx context.Context, payload *ExpensesPayload) error
	LoadExpensesExecution(ctx context.Context, payload *ExpensesExecutionPayload) error
}
