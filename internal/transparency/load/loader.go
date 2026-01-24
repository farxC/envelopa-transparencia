package load

import (
	"context"
	"strconv"
	"strings"
	"time"

	"github.com/farxc/transparency_wrapper/internal/logger"
	"github.com/farxc/transparency_wrapper/internal/store"
	"github.com/farxc/transparency_wrapper/internal/transparency/types"
)

func parseDate(dateStr string) (time.Time, error) {
	if dateStr == "" {
		return time.Time{}, nil
	}
	// Try dd/mm/yyyy format first
	t, err := time.Parse("02/01/2006", dateStr)
	if err == nil {
		return t, nil
	}
	// Fallback to yyyy-mm-dd just in case
	return time.Parse("2006-01-02", dateStr)
}

func parseFloat(valStr string) (float64, error) {
	if valStr == "" {
		return 0.0, nil
	}
	// Remove thousands separator (.) and replace decimal separator (,) with (.)
	cleanStr := strings.ReplaceAll(valStr, ".", "")
	cleanStr = strings.ReplaceAll(cleanStr, ",", ".")
	return strconv.ParseFloat(cleanStr, 64)
}

func parseInt16(val int) int16 {
	return int16(val)
}

func parseInt64(valStr string) (int64, error) {
	if valStr == "" {
		return 0, nil
	}
	return strconv.ParseInt(valStr, 10, 64)
}

func LoadPayload(ctx context.Context, payload *types.CommitmentPayload, storage *store.Storage, appLogger *logger.Logger) error {
	const component = "Loader"
	appLogger.Info(component, "Starting data load for extraction date: %s", payload.ExtractionDate)

	for _, unit := range payload.UnitCommitments {
		// Process Commitments
		for _, c := range unit.Commitments {
			emissionDate, err := parseDate(c.EmitionDate)
			if err != nil {
				appLogger.Warn(component, "Failed to parse emission date for commitment %s: %v", c.CommitmentCode, err)
				continue
			}

			origVal, _ := parseFloat(c.CommitmentOriginalValue)
			convVal, _ := parseFloat(c.CommitmentValueConvertedToBRL)
			convUsed, _ := parseFloat(c.ConversionValueUsed)

			commitment := &store.Commitment{
				CommitmentCode:                c.CommitmentCode,
				ResumedCommitmentCode:         c.ResumedCommitmentCode,
				EmissionDate:                  emissionDate,
				Type:                          c.Type,
				Process:                       c.Process,
				ManagementUnitName:            c.ManagementUnitName,
				ManagementUnitCode:            c.ManagementUnitCode,
				ManagementCode:                c.ManagementCode,
				ManagementName:                c.ManagementName,
				FavoredName:                   c.FavoredName,
				ExpenseNature:                 c.ExpenseNature,
				ExpenseNatureCode:             c.CompleteExpenseNature, // Mapping CompleteExpenseNature to ExpenseNatureCode based on models.go inference
				BudgetPlan:                    c.BudgetPlan,
				CommitmentOriginalValue:       origVal,
				CommitmentValueConvertedToBrl: convVal,
				ConversionValueUsed:           convUsed,
			}

			if err := storage.Commitment.InsertCommitment(ctx, commitment); err != nil {
				appLogger.Error(component, "Failed to insert commitment %s: %v", c.CommitmentCode, err)
				continue
			}

			// Process Commitment Items
			for _, item := range c.Items {
				qty, _ := parseFloat(item.Quantity)
				unitPrice, _ := parseFloat(item.UnitPrice)
				currVal, _ := parseFloat(item.CurrentValue)
				totalPrice, _ := parseFloat(item.TotalPrice)
				currPrice, _ := parseFloat(item.UnitPrice) // Assuming CurrentPrice is same as UnitPrice if not available separately

				commItem := &store.CommitmentItem{
					CommitmentCode:  item.CommitmentCode,
					ExpenseCategory: item.ExpenseCategory,
					ExpenseGroup:    item.ExpenseGroup,
					ExpenseElement:  item.ExpenseElement,
					Description:     item.Description,
					Quantity:        qty,
					Sequential:      parseInt16(item.Sequential),
					UnitPrice:       unitPrice,
					CurrentValue:    currVal,
					CurrentPrice:    currPrice,
					TotalPrice:      totalPrice,
				}

				if err := storage.Commitment.InsertCommitmentItem(ctx, commItem); err != nil {
					appLogger.Error(component, "Failed to insert commitment item for %s: %v", c.CommitmentCode, err)
				}

				// Process Item History
				for _, hist := range item.History {
					opDate, _ := parseDate(hist.OperationDate)
					histQty, _ := parseFloat(hist.ItemQuantity)
					histUnitPrice, _ := parseFloat(hist.ItemUnitPrice)
					histTotalPrice, _ := parseFloat(hist.ItemTotalPrice)

					commHist := &store.CommitmentItemsHistory{
						CommitmentCode: hist.CommitmentCode,
						OperationType:  hist.OperationType,
						ItemQuantity:   histQty,
						Sequential:     parseInt16(hist.Sequential),
						ItemUnitPrice:  histUnitPrice,
						ItemTotalPrice: histTotalPrice,
						OperationDate:  opDate,
					}

					if err := storage.Commitment.InsertCommitmentItemHistory(ctx, commHist); err != nil {
						appLogger.Error(component, "Failed to insert commitment history for %s: %v", c.CommitmentCode, err)
					}
				}
			}
		}

		// Process Liquidations
		for _, l := range unit.Liquidations {
			emissionDate, err := parseDate(l.LiquidationEmitionDate)
			if err != nil {
				appLogger.Warn(component, "Failed to parse emission date for liquidation %s: %v", l.LiquidationCode, err)
				continue
			}
			favoredCode, _ := parseInt64(l.FavoredCode)

			liquidation := &store.Liquidation{
				LiquidationCode:         l.LiquidationCode,
				LiquidationCodeResumed:  l.LiquidationCodeResumed,
				LiquidationEmissionDate: emissionDate,
				DocumentCodeType:        l.DocumentCodeType,
				DocumentType:            l.DocumentType,
				ManagementUnitName:      l.ManagementUnitName,
				ManagementUnitCode:      l.ManagementUnitCode,
				ManagementCode:          l.ManagementCode,
				ManagementName:          l.ManagementName,
				FavoredCode:             favoredCode,
				FavoredName:             l.FavoredName,
				Observation:             l.Observation,
			}

			if err := storage.Liquidation.InsertLiquidation(ctx, liquidation); err != nil {
				appLogger.Error(component, "Failed to insert liquidation %s: %v", l.LiquidationCode, err)
				continue
			}

			for _, imp := range l.ImpactedCommitments {
				liqVal, _ := parseFloat(imp.LiquidatedValueBRL)
				regPay, _ := parseFloat(imp.RegisteredPayablesValueBRL)
				cancPay, _ := parseFloat(imp.CanceledPayablesValueBRL)
				outVal, _ := parseFloat(imp.OutstandingValueLiquidatedBRL)

				lic := &store.LiquidationImpactedCommitment{
					CommitmentCode:                imp.CommitmentCode,
					LiquidationCode:               imp.LiquidationCode,
					ExpenseNatureCode:             imp.CompleteExpenseNature,
					Subitem:                       imp.Subitem,
					LiquidatedValueBRL:            liqVal,
					RegisteredPayablesValueBRL:    regPay,
					CanceledPayablesValueBRL:      cancPay,
					OutstandingValueLiquidatedBRL: outVal,
				}
				if err := storage.Liquidation.InsertLiquidationImpactedCommitment(ctx, lic); err != nil {
					appLogger.Error(component, "Failed to insert liquidation impacted commitment %s: %v", imp.CommitmentCode, err)
				}
			}
		}

		// Process Payments
		for _, p := range unit.Payments {
			emissionDate, err := parseDate(p.PaymentEmitionDate)
			if err != nil {
				appLogger.Warn(component, "Failed to parse emission date for payment %s: %v", p.PaymentCode, err)
				continue
			}
			favoredCode, _ := parseInt64(p.FavoredCode)
			origVal, _ := parseFloat(p.OriginalPaymentValue)
			convVal, _ := parseFloat(p.ConvertedPaymentValue)
			convUsed, _ := parseFloat(p.ConversionUsedValue)

			// Map ExtraBudgetary string "Sim"/"Não" to bool if needed, or just handle basic string.
			// Model has bool `db:"extra_budgetary"`.
			extraBudgetary := false
			if strings.EqualFold(p.ExtraBudgetary, "Sim") || strings.EqualFold(p.ExtraBudgetary, "Yes") || p.ExtraBudgetary == "1" {
				extraBudgetary = true
			}

			payment := &store.Payment{
				PaymentCode:           p.PaymentCode,
				PaymentCodeResumed:    p.PaymentCodeResumed,
				PaymentEmissionDate:   emissionDate,
				DocumentCodeType:      p.DocumentCodeType,
				DocumentType:          p.DocumentType,
				FavoredCode:           favoredCode,
				FavoredName:           p.FavoredName,
				ManagementUnitName:    p.ManagementUnitName,
				ManagementUnitCode:    p.ManagementUnitCode,
				ManagementCode:        p.ManagementCode,
				ManagementName:        p.ManagementName,
				ExtraBudgetary:        extraBudgetary,
				Process:               p.Process,
				OriginalPaymentValue:  origVal,
				ConvertedPaymentValue: convVal,
				ConversionUsedValue:   convUsed,
			}

			if err := storage.Payment.InsertPayment(ctx, payment); err != nil {
				appLogger.Error(component, "Failed to insert payment %s: %v", p.PaymentCode, err)
				continue
			}

			for _, imp := range p.ImpactedCommitments {
				paidVal, _ := parseFloat(imp.PaidValueBRL)
				regPay, _ := parseFloat(imp.RegisteredPayablesValueBRL)
				cancPay, _ := parseFloat(imp.CanceledPayablesValueBRL)
				outVal, _ := parseFloat(imp.OutstandingValuePaidBRL)

				pic := &store.PaymentImpactedCommitment{
					CommitmentCode:             imp.CommitmentCode,
					PaymentCode:                imp.PaymentCode,
					ExpenseNatureCodeComplete:  imp.CompleteExpenseNature,
					Subitem:                    imp.Subitem,
					PaidValueBRL:               paidVal,
					RegisteredPayablesValueBRL: regPay,
					CanceledPayablesValueBRL:   cancPay,
					OutstandingValuePaidBRL:    outVal,
				}
				if err := storage.Payment.InsertPaymentImpactedCommitment(ctx, pic); err != nil {
					appLogger.Error(component, "Failed to insert payment impacted commitment %s: %v", imp.CommitmentCode, err)
				}
			}
		}
	}
	appLogger.Info(component, "Data load completed for extraction date: %s", payload.ExtractionDate)
	return nil
}
