package utils

import "testing"

func TestParseFloat(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    float64
		wantErr bool
	}{
		{name: "empty", input: "", want: 0},
		{name: "brazilian decimal", input: "2252,71", want: 2252.71},
		{name: "brazilian thousands", input: "2.252,71", want: 2252.71},
		{name: "dot decimal", input: "2252.71", want: 2252.71},
		{name: "malformed", input: "2252,71,9", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseFloat(tt.input)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error for %q", tt.input)
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error for %q: %v", tt.input, err)
			}
			if got != tt.want {
				t.Fatalf("ParseFloat(%q) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}
