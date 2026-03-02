package model

import (
	"time"

	"github.com/lib/pq"
)

type IngestionHistory struct {
	ID             int64         `json:"id" db:"id"`
	ProcessedAt    time.Time     `json:"processed_at" db:"processed_at"`
	ReferenceDate  time.Time     `json:"reference_date" db:"reference_date"`
	SourceFile     string        `json:"source_file" db:"source_file"`
	TriggerType    string        `json:"trigger_type" db:"trigger_type"`
	ScopeType      string        `json:"scope_type" db:"scope_type"`
	Status         string        `json:"status" db:"status"`
	ProcessedCodes pq.Int64Array `json:"processed_codes" db:"processed_codes" swaggertype:"array,integer"`
}
