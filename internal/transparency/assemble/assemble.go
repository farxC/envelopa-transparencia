package assemble

import (
	"sync"

	"github.com/farxc/transparency_wrapper/internal/store"
	"github.com/farxc/transparency_wrapper/internal/transparency/converter"
	"github.com/farxc/transparency_wrapper/internal/transparency/types"
	"github.com/go-gota/gota/dataframe"
)

type ExpectedDataFrames struct {
	CommitmentsDf                    dataframe.DataFrame
	CommitmentItemsDf                dataframe.DataFrame
	CommitmentHistoryDf              dataframe.DataFrame
	LiquidationDf                    dataframe.DataFrame
	LiquidationImpactedCommitmentsDf dataframe.DataFrame
	PaymentsDf                       dataframe.DataFrame
	PaymentImpactedCommitmentsDf     dataframe.DataFrame
}

/*
This function assembles the expenses data from various dataframes into a comprehensive structure using goroutines
*/
func AssembleExpensesData(dfs ExpectedDataFrames) map[string]*types.UnitCommitments {
	var mu sync.Mutex
	var processingWg sync.WaitGroup
	processingWg.Add(3)

	unitsMap := make(map[string]*types.UnitCommitments)

	// Helper to get or create unit entry
	getOrCreateUnit := func(ugCode, ugName string) *types.UnitCommitments {
		if _, exists := unitsMap[ugCode]; !exists {
			unitsMap[ugCode] = &types.UnitCommitments{
				UgCode:       ugCode,
				UgName:       ugName,
				Commitments:  []store.Commitment{},
				Liquidations: []store.Liquidation{},
				Payments:     []store.Payment{},
			}
		}
		// Update name if it was empty
		if unitsMap[ugCode].UgName == "" && ugName != "" {
			unitsMap[ugCode].UgName = ugName
		}
		return unitsMap[ugCode]
	}

	// Process commitments
	go func() {
		defer processingWg.Done()
		for i := 0; i < dfs.CommitmentsDf.Nrow(); i++ {
			ugCode := dfs.CommitmentsDf.Col("Código Unidade Gestora").Elem(i).String()
			ugName := dfs.CommitmentsDf.Col("Unidade Gestora").Elem(i).String()

			mu.Lock()
			unit := getOrCreateUnit(ugCode, ugName)
			mu.Unlock()

			commitment := converter.DfRowToCommitment(dfs.CommitmentsDf, i)

			// Attach items to this commitment
			if dfs.CommitmentItemsDf.Nrow() > 0 {
				for j := 0; j < dfs.CommitmentItemsDf.Nrow(); j++ {
					itemCommitmentCode := dfs.CommitmentItemsDf.Col("Código Empenho").Elem(j).String()
					if itemCommitmentCode == commitment.CommitmentCode {
						item := converter.DfRowToCommitmentItem(dfs.CommitmentItemsDf, j)

						// Attach history to this item
						if dfs.CommitmentHistoryDf.Nrow() > 0 {
							itemSequential := dfs.CommitmentItemsDf.Col("Sequencial").Elem(j).String()
							for k := 0; k < dfs.CommitmentHistoryDf.Nrow(); k++ {
								histCommitmentCode := dfs.CommitmentHistoryDf.Col("Código Empenho").Elem(k).String()
								histSequential := dfs.CommitmentHistoryDf.Col("Sequencial").Elem(k).String()
								if histCommitmentCode == commitment.CommitmentCode && histSequential == itemSequential {
									history := converter.DfRowToCommitmentItemHistory(dfs.CommitmentHistoryDf, k)
									item.History = append(item.History, history)
								}
							}
						}

						commitment.Items = append(commitment.Items, item)
					}
				}
			}

			mu.Lock()
			unit.Commitments = append(unit.Commitments, commitment)
			mu.Unlock()
		}
	}()
	// Process liquidations
	go func() {
		defer processingWg.Done()
		for i := 0; i < dfs.LiquidationDf.Nrow(); i++ {
			ugCode := dfs.LiquidationDf.Col("Código Unidade Gestora").Elem(i).String()
			ugName := dfs.LiquidationDf.Col("Unidade Gestora").Elem(i).String()

			mu.Lock()
			unit := getOrCreateUnit(ugCode, ugName)
			mu.Unlock()

			liquidation := converter.DfRowToLiquidation(dfs.LiquidationDf, i)
			if dfs.LiquidationImpactedCommitmentsDf.Nrow() > 0 {
				for j := 0; j < dfs.LiquidationImpactedCommitmentsDf.Nrow(); j++ {
					impactedLiquidationCode := dfs.LiquidationImpactedCommitmentsDf.Col("Código Liquidação").Elem(j).String()
					if impactedLiquidationCode == liquidation.LiquidationCode {
						impactedCommitment := converter.DfRowToLiquidationImpactedCommitment(dfs.LiquidationImpactedCommitmentsDf, j)
						liquidation.ImpactedCommitments = append(liquidation.ImpactedCommitments, impactedCommitment)
					}
				}
			}
			mu.Lock()
			unit.Liquidations = append(unit.Liquidations, liquidation)
			mu.Unlock()
		}
	}()

	// Process payments
	go func() {
		defer processingWg.Done()
		for i := 0; i < dfs.PaymentsDf.Nrow(); i++ {
			ugCode := dfs.PaymentsDf.Col("Código Unidade Gestora").Elem(i).String()
			ugName := dfs.PaymentsDf.Col("Unidade Gestora").Elem(i).String()
			mu.Lock()
			unit := getOrCreateUnit(ugCode, ugName)
			mu.Unlock()
			payment := converter.DfRowToPayment(dfs.PaymentsDf, i)
			if dfs.PaymentImpactedCommitmentsDf.Nrow() > 0 {
				for j := 0; j < dfs.PaymentImpactedCommitmentsDf.Nrow(); j++ {
					impactedPaymentCode := dfs.PaymentImpactedCommitmentsDf.Col("Código Pagamento").Elem(j).String()
					if impactedPaymentCode == payment.PaymentCode {
						impactedCommitment := converter.DfRowToPaymentImpactedCommitment(dfs.PaymentImpactedCommitmentsDf, j)
						payment.ImpactedCommitments = append(payment.ImpactedCommitments, impactedCommitment)
					}
				}
			}
			mu.Lock()
			unit.Payments = append(unit.Payments, payment)
			mu.Unlock()
		}

	}()
	processingWg.Wait()

	return unitsMap
}