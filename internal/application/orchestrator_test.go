package application

import (
	"testing"
	"time"

	"github.com/farxc/envelopa-transparencia/internal/domain/model"
)

func TestShouldProcess(t *testing.T) {
	o := NewOrchestrator[struct{}](nil, nil, nil, 1)
	o.statusMap = map[string]model.IngestionHistory{
		"success":      {Status: statusSuccess, ProcessedAt: time.Now()},
		"skipped":      {Status: statusSkipped, ProcessedAt: time.Now()},
		"failure":      {Status: statusFailure, ProcessedAt: time.Now()},
		"in-progress":  {Status: statusInProgress, ProcessedAt: time.Now()},
		"stale-in-pro": {Status: statusInProgress, ProcessedAt: time.Now().Add(-time.Hour)},
	}

	tests := []struct {
		key   string
		force bool
		want  bool
	}{
		{"unknown", false, true},
		{"success", false, false},
		{"skipped", false, false},
		{"failure", false, true},
		{"in-progress", false, false},
		{"stale-in-pro", false, true},

		// -force reprocesses finished jobs but never one another process is
		// running right now.
		{"unknown", true, true},
		{"success", true, true},
		{"skipped", true, true},
		{"failure", true, true},
		{"in-progress", true, false},
		{"stale-in-pro", true, true},
	}
	for _, tt := range tests {
		if got := o.ShouldProcess(tt.key, tt.force); got != tt.want {
			t.Errorf("ShouldProcess(%q, force=%t) = %t, want %t", tt.key, tt.force, got, tt.want)
		}
	}
}
