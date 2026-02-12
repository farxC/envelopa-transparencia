package load

import (
	"context"
	"fmt"
	"time"

	"github.com/farxc/envelopa-transparencia/internal/logger"
	"github.com/farxc/envelopa-transparencia/internal/store"
	"github.com/farxc/envelopa-transparencia/internal/transparency/types"
)

func LoadPayload(ctx context.Context, payload *types.CommitmentPayload, storage *store.Storage, appLogger *logger.Logger) error {
	const component = "Loader"
	appLogger.Info(component, "Starting data load for extraction date: %s", payload.ExtractionDate)

	for _, unit := range payload.UnitCommitments {
		// Process Commitments
		for _, c := range unit.Commitments {
			now := time.Now()
			commitment := c
			commitment.InsertedAt = now
			commitment.UpdatedAt = now

			if err := storage.Commitment.InsertCommitment(ctx, &commitment); err != nil {
				appLogger.Error(component, "Failed to insert commitment %s (ID %d): %v", commitment.CommitmentCode, commitment.ID, err)
				continue
			}

			// Process Commitment Items
			for _, item := range commitment.Items {
				item.InsertedAt = now
				item.UpdatedAt = now

				if err := storage.Commitment.InsertCommitmentItem(ctx, &item); err != nil {
					appLogger.Error(component, "Failed to insert commitment item for %s: %v", commitment.CommitmentCode, err)
				}

				for _, hist := range item.History {
					hist.InsertedAt = now
					hist.UpdatedAt = now

					if err := storage.Commitment.InsertCommitmentItemHistory(ctx, &hist); err != nil {
						appLogger.Error(component, "Failed to insert commitment history for %s: %v", commitment.CommitmentCode, err)
					}
				}
			}
		}

		// // Process Liquidations
		for _, l := range unit.Liquidations {
			liquidation := l
			liquidation.InsertedAt = time.Now()
			liquidation.UpdatedAt = time.Now()

			if err := storage.Liquidation.InsertLiquidation(ctx, &liquidation); err != nil {
				appLogger.Error(component, "Failed to insert liquidation %s: %v", l.LiquidationCode, err)
				continue
			}
			fmt.Printf("Liquidation: %+v\n", liquidation)

			for _, imp := range liquidation.ImpactedCommitments {
				imp.InsertedAt = time.Now()
				imp.UpdatedAt = time.Now()

				fmt.Printf("Lic: %+v\n", imp)
				if err := storage.Liquidation.InsertLiquidationImpactedCommitment(ctx, &imp); err != nil {
					appLogger.Error(component, "Failed to insert liquidation impacted commitment %s: %v", imp.CommitmentCode, err)
				}
			}
		}

		// // Process Payments
		for _, p := range unit.Payments {
			payment := p
			payment.InsertedAt = time.Now()
			payment.UpdatedAt = time.Now()

			fmt.Printf("Payment: %+v\n", payment)
			if err := storage.Payment.InsertPayment(ctx, &payment); err != nil {
				appLogger.Error(component, "Failed to insert payment %s: %v", p.PaymentCode, err)
				continue
			}

			for _, imp := range payment.ImpactedCommitments {
				imp.InsertedAt = time.Now()
				imp.UpdatedAt = time.Now()

				fmt.Printf("Pic: %+v\n", imp)
				if err := storage.Payment.InsertPaymentImpactedCommitment(ctx, &imp); err != nil {
					appLogger.Error(component, "Failed to insert payment impacted commitment %s: %v", imp.CommitmentCode, err)
				}
			}
		}
	}
	appLogger.Info(component, "Data load completed for extraction date: %s", payload.ExtractionDate)
	return nil
}
