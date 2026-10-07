package application

import (
	"context"
	"testing"
	"time"

	"github.com/farxc/envelopa-transparencia/internal/domain/model"
	"github.com/farxc/envelopa-transparencia/internal/domain/repository"
	"github.com/farxc/envelopa-transparencia/internal/infrastructure/logger"
)

// fakeHistory returns fixed ingestion records from GetHistoryInRange.
type fakeHistory struct {
	repository.IngestionHistoryInterface
	records []model.IngestionHistory
}

func (f fakeHistory) GetHistoryInRange(context.Context, time.Time, time.Time, []int64, string) ([]model.IngestionHistory, error) {
	return f.records, nil
}

var quietLogger = &logger.Logger{MinLevel: logger.LevelError}

// history builds one record per status, each for its own reference date, as
// the pipelines' BuildHistoryRecord would store them.
func history(refs map[string]time.Time) []model.IngestionHistory {
	var out []model.IngestionHistory
	for status, ref := range refs {
		at := time.Now()
		if status == "STALE" {
			status, at = statusInProgress, time.Now().Add(-time.Hour)
		}
		out = append(out, model.IngestionHistory{ReferenceDate: ref, Status: status, ProcessedAt: at})
	}
	return out
}

type runCase struct {
	status       string // history status of the job's reference; "" = no record
	debug, force bool
	want         bool
}

// Expected decisions for a kind whose finished jobs are skipped unless forced.
var skipFinishedCases = []runCase{
	{"", false, false, true},
	{statusSuccess, false, false, false},
	{statusSkipped, false, false, false},
	{statusFailure, false, false, true},
	{statusInProgress, false, false, false},
	{"STALE", false, false, true},
	{statusSuccess, false, true, true},
	{statusSkipped, false, true, true},
	{statusInProgress, false, true, false},
	{statusSuccess, true, false, true},
	{statusInProgress, true, false, true},
}

// Expected decisions for kinds whose files change daily (budget, expenses
// execution): finished jobs run again even without -force; a job in progress
// in another run is still skipped.
var alwaysReloadCases = []runCase{
	{"", false, false, true},
	{statusSuccess, false, false, true},
	{statusSkipped, false, false, true},
	{statusFailure, false, false, true},
	{statusInProgress, false, false, false},
	{"STALE", false, false, true},
	{statusSuccess, false, true, true},
	{statusInProgress, false, true, false},
	{statusInProgress, true, false, true},
}

func checkShouldRun[J any](t *testing.T, p Pipeline[J], cases []runCase, jobs map[string]J, refs map[string]time.Time) {
	t.Helper()
	o := NewOrchestrator[J](p, fakeHistory{records: history(refs)}, quietLogger, 1)
	if err := o.InitializeState(context.Background(), time.Time{}, time.Time{}, nil); err != nil {
		t.Fatalf("InitializeState: %v", err)
	}
	for _, c := range cases {
		job, ok := jobs[c.status]
		if !ok {
			t.Fatalf("no job for status %q", c.status)
		}
		if got := o.ShouldRun(job, c.debug, c.force); got != c.want {
			t.Errorf("%s: status=%q debug=%t force=%t: ShouldRun = %t, want %t",
				p.Kind(), c.status, c.debug, c.force, got, c.want)
		}
	}
}

func day(d int) time.Time { return time.Date(2025, 3, d, 0, 0, 0, 0, time.UTC) }

func TestShouldRun_ExpensesDaily(t *testing.T) {
	statuses := []string{"", statusSuccess, statusSkipped, statusFailure, statusInProgress, "STALE"}
	jobs := map[string]model.ExpensesDailyJob{}
	refs := map[string]time.Time{}
	for i, s := range statuses {
		jobs[s] = model.ExpensesDailyJob{Date: day(i + 1)}
		if s != "" {
			refs[s] = day(i + 1)
		}
	}
	checkShouldRun[model.ExpensesDailyJob](t, NewExpensesDailyPipeline(nil, nil, quietLogger), skipFinishedCases, jobs, refs)
}

func TestShouldRun_ExpensesExecution(t *testing.T) {
	statuses := []string{"", statusSuccess, statusSkipped, statusFailure, statusInProgress, "STALE"}
	jobs := map[string]model.ExpensesExecutionJob{}
	refs := map[string]time.Time{}
	for i, s := range statuses {
		m := time.Date(2025, time.Month(i+1), 1, 0, 0, 0, 0, time.UTC)
		jobs[s] = model.ExpensesExecutionJob{Year: m.Format("2006"), Month: m.Format("01")}
		if s != "" {
			refs[s] = m
		}
	}
	checkShouldRun[model.ExpensesExecutionJob](t, NewExpensesExecutionPipeline(nil, nil, quietLogger), alwaysReloadCases, jobs, refs)
}

func TestShouldRun_Budget(t *testing.T) {
	statuses := []string{"", statusSuccess, statusSkipped, statusFailure, statusInProgress, "STALE"}
	jobs := map[string]model.BudgetJob{}
	refs := map[string]time.Time{}
	for i, s := range statuses {
		y := time.Date(2020+i, 1, 1, 0, 0, 0, 0, time.UTC)
		jobs[s] = model.BudgetJob{Year: y.Format("2006")}
		if s != "" {
			refs[s] = y
		}
	}
	checkShouldRun[model.BudgetJob](t, NewBudgetPipeline(nil, nil, quietLogger), alwaysReloadCases, jobs, refs)
}
