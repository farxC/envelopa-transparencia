ALTER TABLE expenses_execution ALTER COLUMN action_code TYPE VARCHAR(255) USING action_code::VARCHAR;
