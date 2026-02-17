-- Add unique constraint to commitment_items
ALTER TABLE commitment_items ADD CONSTRAINT commitment_items_commitment_code_sequential_key UNIQUE (commitment_code, sequential);

-- Add unique constraint to commitment_items_history
-- We include operation_date and operation_type to allow multiple history entries for the same item sequential,
-- but prevent duplicates of the exact same history entry during ETL re-runs.
ALTER TABLE commitment_items_history ADD CONSTRAINT commitment_items_history_pk UNIQUE (commitment_code, sequential, operation_date, operation_type);

-- Add unique constraint to payment_impacted_commitments
ALTER TABLE payment_impacted_commitments ADD CONSTRAINT payment_impacted_commitments_pk UNIQUE (payment_code, commitment_code, expense_nature_code_complete, subitem);

-- Add unique constraint to liquidation_impacted_commitments
ALTER TABLE liquidation_impacted_commitments ADD CONSTRAINT liquidation_impacted_commitments_pk UNIQUE (liquidation_code, commitment_code, expense_nature_code_complete, subitem);
