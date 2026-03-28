package service

import (
	"fmt"

	"github.com/farxc/envelopa-transparencia/internal/domain/model"
)

/*
AssembleExpensesData assembles raw domain entities into a comprehensive hierarchical structure.
It takes flat slices of all entities and groups them logically.
*/
func AssembleExpensesData(
	commitments []model.Commitment,
	items []model.CommitmentItem,
	history []model.CommitmentItemsHistory,
	liquidations []model.Liquidation,
	liImpacts []model.LiquidationImpactedCommitment,
	payments []model.Payment,
	paImpacts []model.PaymentImpactedCommitment,
) map[string]*UnitsExpenses {
	unitsMap := make(map[string]*UnitsExpenses)

	// 1. Group History by CommitmentCode + Sequential
	historyMap := make(map[string][]model.CommitmentItemsHistory)
	for _, h := range history {
		key := fmt.Sprintf("%s-%d", h.CommitmentCode, h.Sequential)
		historyMap[key] = append(historyMap[key], h)
	}

	// 2. Group Items by CommitmentCode (and attach history)
	itemsMap := make(map[string][]model.CommitmentItem)
	for _, item := range items {
		histKey := fmt.Sprintf("%s-%d", item.CommitmentCode, item.Sequential)
		if h, ok := historyMap[histKey]; ok {
			item.History = h
		}
		itemsMap[item.CommitmentCode] = append(itemsMap[item.CommitmentCode], item)
	}

	// 3. Group Liquidation Impacts by LiquidationCode
	liImpactMap := make(map[string][]model.LiquidationImpactedCommitment)
	for _, imp := range liImpacts {
		liImpactMap[imp.LiquidationCode] = append(liImpactMap[imp.LiquidationCode], imp)
	}

	// 4. Index Commitments by CommitmentCode for payment impact unit lookup
	commitmentMap := make(map[string]model.Commitment)
	for _, commitment := range commitments {
		commitmentMap[commitment.CommitmentCode] = commitment
	}

	// Helper to get or create unit entry
	getOrCreateUnit := func(ugCode int, ugName string) *UnitsExpenses {
		key := fmt.Sprintf("%d", ugCode)
		if _, exists := unitsMap[key]; !exists {
			unitsMap[key] = &UnitsExpenses{
				UgCode:                     key,
				UgName:                     ugName,
				Commitments:                []model.Commitment{},
				Liquidations:               []model.Liquidation{},
				Payments:                   []model.Payment{},
				PaymentImpactedCommitments: []model.PaymentImpactedCommitment{},
			}
		}
		if unitsMap[key].UgName == "" && ugName != "" {
			unitsMap[key].UgName = ugName
		}
		return unitsMap[key]
	}

	// 6. Build Hierarchy
	for _, c := range commitments {
		if its, ok := itemsMap[c.CommitmentCode]; ok {
			c.Items = its
		}
		unit := getOrCreateUnit(c.ManagementUnitCode, c.ManagementUnitName)
		unit.Commitments = append(unit.Commitments, c)
	}

	for _, l := range liquidations {
		if imps, ok := liImpactMap[l.LiquidationCode]; ok {
			l.ImpactedCommitments = imps
		}
		unit := getOrCreateUnit(l.ManagementUnitCode, l.ManagementUnitName)
		unit.Liquidations = append(unit.Liquidations, l)
	}

	for _, p := range payments {
		unit := getOrCreateUnit(p.ManagementUnitCode, p.ManagementUnitName)
		unit.Payments = append(unit.Payments, p)
	}

	for _, imp := range paImpacts {
		commitment, ok := commitmentMap[imp.CommitmentCode]
		if !ok {
			continue
		}
		unit := getOrCreateUnit(commitment.ManagementUnitCode, commitment.ManagementUnitName)
		unit.PaymentImpactedCommitments = append(unit.PaymentImpactedCommitments, imp)
	}

	return unitsMap
}
