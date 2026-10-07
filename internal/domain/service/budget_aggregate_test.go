package service

import (
	"math"
	"testing"

	"github.com/farxc/envelopa-transparencia/internal/domain/model"
)

func budgetRow(action string, element int64, initial, updated, committed, executed float64) model.ExpenseBudget {
	return model.ExpenseBudget{
		Exercise: 2025, SubordinateAgencyCode: 26421, SubordinateAgencyName: "IFRO", BudgetaryUnitCode: 26421,
		FunctionCode: 12, SubfunctionCode: 363, BudgetProgramCode: "5112", ActionCode: action, ActionName: "Ação " + action,
		EconomicCategoryCode: 3, ExpenseGroupCode: 3, ExpenseElementCode: element,
		InitialBudget: initial, UpdatedBudget: updated, CommittedBudget: committed, ExecutedBudget: executed,
		PercentExecutedBudget: BudgetPercentExecuted(updated, executed),
	}
}

func TestAggregateBudgetRows(t *testing.T) {
	rows := []model.ExpenseBudget{
		budgetRow("20RL", 39, 100, 200, 150, 50),
		budgetRow("2994", 18, 10, 10, 10, 10),
		budgetRow("20RL", 39, 1, 300, 50, 250), // same key as the first row
		budgetRow("20RL", 30, 5, 0, 0, 0),
	}

	got := AggregateBudgetRows(rows)

	if len(got) != 3 {
		t.Fatalf("got %d rows, want 3: %+v", len(got), got)
	}
	// First-seen order is kept.
	first := got[0]
	if first.ActionCode != "20RL" || first.ExpenseElementCode != 39 {
		t.Fatalf("first row is %s/%d, want 20RL/39", first.ActionCode, first.ExpenseElementCode)
	}
	if first.InitialBudget != 101 || first.UpdatedBudget != 500 || first.CommittedBudget != 200 || first.ExecutedBudget != 300 {
		t.Errorf("summed values = %v/%v/%v/%v, want 101/500/200/300",
			first.InitialBudget, first.UpdatedBudget, first.CommittedBudget, first.ExecutedBudget)
	}
	if math.Abs(first.PercentExecutedBudget-60) > 1e-9 {
		t.Errorf("percent = %v, want 60 (recomputed from the sums)", first.PercentExecutedBudget)
	}
	if first.ActionName != "Ação 20RL" || first.SubordinateAgencyName != "IFRO" {
		t.Errorf("names not kept: %q %q", first.ActionName, first.SubordinateAgencyName)
	}
	if got[1].ActionCode != "2994" || got[1].InitialBudget != 10 {
		t.Errorf("distinct key mixed up: %+v", got[1])
	}
	if got[2].PercentExecutedBudget != 0 {
		t.Errorf("percent with zero updated budget = %v, want 0", got[2].PercentExecutedBudget)
	}
}

func TestAggregateBudgetRows_DoesNotModifyInput(t *testing.T) {
	rows := []model.ExpenseBudget{budgetRow("20RL", 39, 1, 1, 1, 1), budgetRow("20RL", 39, 1, 1, 1, 1)}
	AggregateBudgetRows(rows)
	if rows[0].InitialBudget != 1 {
		t.Errorf("input row was modified: %+v", rows[0])
	}
}
