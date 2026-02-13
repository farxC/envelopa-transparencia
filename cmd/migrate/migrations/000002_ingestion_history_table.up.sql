CREATE TABLE ingestion_history (
    id SERIAL PRIMARY KEY,
    
    -- Quando o job rodou
    processed_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    -- A data de referência dos dados (ex: dados do dia 03/01)
    reference_date DATE NOT NULL,
    
    -- O nome do arquivo ZIP processado (Rastreabilidade)
    source_file VARCHAR(255), 

    trigger_type VARCHAR(50) CHECK (trigger_type IN ('MANUAL', 'SCHEDULED')) NOT NULL,

    scope_type VARCHAR(50) CHECK (scope_type IN ('MANAGEMENT_UNIT', 'MANAGEMENT')) NOT NULL,
    
    -- Status geral do lote ('SUCCESS', 'PARTIAL', 'FAILURE', 'IN_PROGRESS', 'SKIPPED')
    status VARCHAR(20) CHECK (status IN ('SUCCESS', 'PARTIAL', 'FAILURE', 'IN_PROGRESS', 'SKIPPED')) NOT NULL,
    
    -- O ARRAY MÁGICO (contendo os IDs das UGs que foram processadas com sucesso)
    -- Ex: {154032, 158123, ...}
    processed_codes INTEGER[]
    
);

CREATE INDEX idx_ingest_processed_codes ON ingestion_history USING GIN (processed_codes);
CREATE INDEX idx_ingest_ref_date ON ingestion_history (reference_date);