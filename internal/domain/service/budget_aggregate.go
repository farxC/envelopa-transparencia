package service

// BudgetPercentExecuted returns executed as a percentage of the updated budget,
// or 0 when there is no updated budget. It matches the percentage the budget
// summary queries compute from the summed values.
func BudgetPercentExecuted(updated, executed float64) float64 {
	if updated <= 0 {
		return 0
	}
	return executed / updated * 100
}
