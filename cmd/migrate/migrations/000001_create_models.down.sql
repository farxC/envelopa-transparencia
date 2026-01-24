-- Drop tables in reverse order of creation (respecting foreign key dependencies)
DROP TABLE IF EXISTS liquidation_impacted_commitments;
DROP TABLE IF EXISTS liquidations;
DROP TABLE IF EXISTS payment_impacted_commitments;
DROP TABLE IF EXISTS payments;
DROP TABLE IF EXISTS commitment_items_history;
DROP TABLE IF EXISTS commitment_items;
DROP TABLE IF EXISTS commitments;
