package service

import "github.com/farxc/envelopa-transparencia/internal/domain/model"

// BudgetPercentExecuted returns executed as a percentage of the updated budget,
// or 0 when there is no updated budget. It matches the percentage the budget
// summary queries compute from the summed values.
func BudgetPercentExecuted(updated, executed float64) float64 {
	if updated <= 0 {
		return 0
	}
	return executed / updated * 100
}

// budgetKey is the unique key of expense_budget (the ON CONFLICT target).
type budgetKey struct {
	exercise          int
	subordinateAgency int64
	budgetaryUnit     int64
	function          int64
	subfunction       int64
	budgetProgram     string
	action            string
	economicCategory  int64
	expenseGroup      int16
	expenseElement    int64
}

// AggregateBudgetRows sums the values of rows that share the expense_budget
// unique key, so the load does not keep only the last of them. Names come from
// the first row of each key, the percentage is recomputed from the sums and
// the first-seen order is kept. The input is not modified.
func AggregateBudgetRows(rows []model.ExpenseBudget) []model.ExpenseBudget {
	index := make(map[budgetKey]int, len(rows))
	out := make([]model.ExpenseBudget, 0, len(rows))
	for _, r := range rows {
		k := budgetKey{
			r.Exercise, r.SubordinateAgencyCode, r.BudgetaryUnitCode, r.FunctionCode, r.SubfunctionCode,
			r.BudgetProgramCode, r.ActionCode, r.EconomicCategoryCode, r.ExpenseGroupCode, r.ExpenseElementCode,
		}
		i, ok := index[k]
		if !ok {
			index[k] = len(out)
			out = append(out, r)
			continue
		}
		out[i].InitialBudget += r.InitialBudget
		out[i].UpdatedBudget += r.UpdatedBudget
		out[i].CommittedBudget += r.CommittedBudget
		out[i].ExecutedBudget += r.ExecutedBudget
	}
	for i := range out {
		out[i].PercentExecutedBudget = BudgetPercentExecuted(out[i].UpdatedBudget, out[i].ExecutedBudget)
	}
	return out
}
