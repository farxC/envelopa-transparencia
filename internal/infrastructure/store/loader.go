package store

import (
	"context"
	"time"

	"github.com/farxc/envelopa-transparencia/internal/domain/service"
	"github.com/farxc/envelopa-transparencia/internal/infrastructure/logger"
)

func LoadPayload(ctx context.Context, payload *service.ExpensesPayload, storage *Storage, appLogger *logger.Logger) error {
	const component = "Loader"
	appLogger.Info(component, "Starting data load for extraction date: %s", payload.ExtractionDate)

	for _, unit := range payload.UnitsExpenses {
		err := func() error {
			tx, err := storage.DB.BeginTxx(ctx, nil)
			if err != nil {
				appLogger.Error(component, "Failed to start transaction: %v", err)
				return err
			}
			defer tx.Rollback()
			txStorage := storage.WithTx(tx)

			// Process Commitments
			for _, c := range unit.Commitments {
				now := time.Now()
				commitment := c
				commitment.InsertedAt = now
				commitment.UpdatedAt = now

				if err := txStorage.Commitment.InsertCommitment(ctx, &commitment); err != nil {
					appLogger.Error(component, "Failed to insert commitment %s (ID %d): %v", commitment.CommitmentCode, commitment.ID, err)
					return err
				}

				if store, ok := txStorage.Commitment.(*CommitmentStore); ok {
					if err := store.DeleteCommitmentChildren(ctx, commitment.CommitmentCode); err != nil {
						appLogger.Error(component, "Failed to reconcile commitment children for %s: %v", commitment.CommitmentCode, err)
						return err
					}
				}

				// Process Commitment Items
				for _, item := range commitment.Items {
					item.InsertedAt = now
					item.UpdatedAt = now

					if err := txStorage.Commitment.InsertCommitmentItem(ctx, &item); err != nil {
						appLogger.Error(component, "Failed to insert commitment item for %s: %v", commitment.CommitmentCode, err)
						return err
					}

					for _, hist := range item.History {
						hist.InsertedAt = now
						hist.UpdatedAt = now

						if err := txStorage.Commitment.InsertCommitmentItemHistory(ctx, &hist); err != nil {
							appLogger.Error(component, "Failed to insert commitment history for %s: %v", commitment.CommitmentCode, err)
							return err
						}
					}
				}
			}

			// // Process Liquidations
			for _, l := range unit.Liquidations {
				liquidation := l
				liquidation.InsertedAt = time.Now()
				liquidation.UpdatedAt = time.Now()

				if err := txStorage.Liquidation.InsertLiquidation(ctx, &liquidation); err != nil {
					appLogger.Error(component, "Failed to insert liquidation %s: %v", l.LiquidationCode, err)
					return err
				}

				if store, ok := txStorage.Liquidation.(*LiquidationStore); ok {
					if err := store.DeleteImpactedCommitments(ctx, liquidation.LiquidationCode); err != nil {
						appLogger.Error(component, "Failed to reconcile liquidation impacts for %s: %v", liquidation.LiquidationCode, err)
						return err
					}
				}

				for _, imp := range liquidation.ImpactedCommitments {
					imp.InsertedAt = time.Now()
					imp.UpdatedAt = time.Now()

					if err := txStorage.Liquidation.InsertLiquidationImpactedCommitment(ctx, &imp); err != nil {
						appLogger.Error(component, "Failed to insert liquidation impacted commitment %s: %v", imp.CommitmentCode, err)
						return err
					}
				}
			}

			// // Process Payments
			for _, p := range unit.Payments {
				payment := p
				payment.InsertedAt = time.Now()
				payment.UpdatedAt = time.Now()

				if err := txStorage.Payment.InsertPayment(ctx, &payment); err != nil {
					appLogger.Error(component, "Failed to insert payment %s: %v", p.PaymentCode, err)
					return err
				}

				if store, ok := txStorage.Payment.(*PaymentStore); ok {
					if err := store.DeleteImpactedCommitments(ctx, payment.PaymentCode); err != nil {
						appLogger.Error(component, "Failed to reconcile payment impacts for %s: %v", payment.PaymentCode, err)
						return err
					}
				}
			}

			for _, imp := range unit.PaymentImpactedCommitments {
				imp.InsertedAt = time.Now()
				imp.UpdatedAt = time.Now()

				if err := txStorage.Payment.InsertPaymentImpactedCommitment(ctx, &imp); err != nil {
					appLogger.Error(component, "Failed to insert payment impacted commitment %s: %v", imp.CommitmentCode, err)
					return err
				}
			}
			return tx.Commit()
		}()
		if err != nil {
			return err
		}

	}
	appLogger.Info(component, "Data load completed for extraction date: %s", payload.ExtractionDate)
	return nil
}
