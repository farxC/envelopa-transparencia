package store

import (
	"context"
	"time"

	"github.com/farxc/envelopa-transparencia/internal/domain/service"
	"github.com/farxc/envelopa-transparencia/internal/infrastructure/logger"
)

type storageLoader struct {
	storage *Storage
	logger  *logger.Logger
}

func NewStorageLoader(storage *Storage, logger *logger.Logger) service.Loader {
	return &storageLoader{
		storage: storage,
		logger:  logger,
	}
}

func (s *storageLoader) LoadExpenses(ctx context.Context, payload *service.ExpensesPayload) error {
	const component = "Loader"
	s.logger.Info(component, "Starting data load for extraction date: %s", payload.ExtractionDate)

	for _, unit := range payload.UnitsExpenses {
		err := func() error {
			tx, err := s.storage.DB.BeginTxx(ctx, nil)
			if err != nil {
				s.logger.Error(component, "Failed to start transaction: %v", err)
				return err
			}
			defer tx.Rollback()
			txStorage := s.storage.WithTx(tx)

			// Process Commitments
			for _, c := range unit.Commitments {
				now := time.Now()
				commitment := c
				commitment.InsertedAt = now
				commitment.UpdatedAt = now

				if err := txStorage.Commitment.InsertCommitment(ctx, &commitment); err != nil {
					s.logger.Error(component, "Failed to insert commitment %s (ID %d): %v", commitment.CommitmentCode, commitment.ID, err)
					return err
				}

				if store, ok := txStorage.Commitment.(*CommitmentStore); ok {
					if err := store.DeleteCommitmentChildren(ctx, commitment.CommitmentCode); err != nil {
						s.logger.Error(component, "Failed to reconcile commitment children for %s: %v", commitment.CommitmentCode, err)
						return err
					}
				}

				// Process Commitment Items
				for _, item := range commitment.Items {
					item.InsertedAt = now
					item.UpdatedAt = now

					if err := txStorage.Commitment.InsertCommitmentItem(ctx, &item); err != nil {
						s.logger.Error(component, "Failed to insert commitment item for %s: %v", commitment.CommitmentCode, err)
						return err
					}

					for _, hist := range item.History {
						hist.InsertedAt = now
						hist.UpdatedAt = now

						if err := txStorage.Commitment.InsertCommitmentItemHistory(ctx, &hist); err != nil {
							s.logger.Error(component, "Failed to insert commitment history for %s: %v", commitment.CommitmentCode, err)
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
					s.logger.Error(component, "Failed to insert liquidation %s: %v", l.LiquidationCode, err)
					return err
				}

				if store, ok := txStorage.Liquidation.(*LiquidationStore); ok {
					if err := store.DeleteImpactedCommitments(ctx, liquidation.LiquidationCode); err != nil {
						s.logger.Error(component, "Failed to reconcile liquidation impacts for %s: %v", liquidation.LiquidationCode, err)
						return err
					}
				}

				for _, imp := range liquidation.ImpactedCommitments {
					imp.InsertedAt = time.Now()
					imp.UpdatedAt = time.Now()

					if err := txStorage.Liquidation.InsertLiquidationImpactedCommitment(ctx, &imp); err != nil {
						s.logger.Error(component, "Failed to insert liquidation impacted commitment %s: %v", imp.CommitmentCode, err)
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
					s.logger.Error(component, "Failed to insert payment %s: %v", p.PaymentCode, err)
					return err
				}

				if store, ok := txStorage.Payment.(*PaymentStore); ok {
					if err := store.DeleteImpactedCommitments(ctx, payment.PaymentCode); err != nil {
						s.logger.Error(component, "Failed to reconcile payment impacts for %s: %v", payment.PaymentCode, err)
						return err
					}
				}
			}

			for _, imp := range unit.PaymentImpactedCommitments {
				imp.InsertedAt = time.Now()
				imp.UpdatedAt = time.Now()

				if err := txStorage.Payment.InsertPaymentImpactedCommitment(ctx, &imp); err != nil {
					s.logger.Error(component, "Failed to insert payment impacted commitment %s: %v", imp.CommitmentCode, err)
					return err
				}
			}
			return tx.Commit()
		}()
		if err != nil {
			return err
		}

	}
	s.logger.Info(component, "Data load completed for extraction date: %s", payload.ExtractionDate)
	return nil
}

func (s *storageLoader) LoadExpensesExecution(ctx context.Context, payload *service.ExpensesExecutionPayload) error {
	const component = "Loader"
	s.logger.Info(component, "Starting expenses execution load for extraction date: %s", payload.ExtractionDate)

	now := time.Now()
	for _, unit := range payload.UnitsExpenses {
		execution := unit.ExpenseExecution
		execution.InsertedAt = now
		execution.UpdatedAt = now

		if err := s.storage.ExpensesExecution.InsertExpenseExecution(ctx, &execution); err != nil {
			s.logger.Error(component, "Failed to insert expense execution for unit %d (%s): %v", unit.UgCode, unit.UgName, err)
			return err
		}
	}

	s.logger.Info(component, "Expenses execution load completed for extraction date: %s", payload.ExtractionDate)
	return nil
}
