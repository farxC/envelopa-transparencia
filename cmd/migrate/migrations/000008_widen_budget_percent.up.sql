-- percent_executed_budget is computed as executed / updated * 100, which can
-- exceed 999.9999 when the updated budget is small.
ALTER TABLE expense_budget ALTER COLUMN percent_executed_budget TYPE NUMERIC(12, 4);
