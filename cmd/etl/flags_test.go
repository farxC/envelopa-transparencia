package main

import (
	"errors"
	"flag"
	"io"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/farxc/envelopa-transparencia/internal/infrastructure/client/portal"
	"github.com/farxc/envelopa-transparencia/internal/infrastructure/logger"
)

func TestParseFlags(t *testing.T) {
	now := time.Date(2026, 3, 22, 15, 0, 0, 0, time.UTC)
	yesterday := time.Date(2026, 3, 21, 0, 0, 0, 0, time.UTC)

	tests := []struct {
		name    string
		args    []string
		want    etlFlags
		wantErr string
	}{
		{
			name: "defaults",
			args: nil,
			want: etlFlags{
				kind:         kindExpensesExecution,
				initDate:     yesterday,
				endDate:      yesterday,
				codes:        defaultCodes,
				trigger:      triggerManual,
				logLevel:     logger.LevelInfo,
				logLevelName: "info",
				concurrency:  10,
				download:     portal.DefaultDownloadOptions(),
			},
		},
		{
			name: "all flags set",
			args: []string{
				"-kind=expenses", "-init=2025-01-01", "-end=2025-01-31",
				"-codes=26421,26415", "-byManagingCode=true", "-trigger=SCHEDULED",
				"-loglevel=debug", "-concurrency=2", "-debug=true", "-force=true",
				"-downloadLimit=50", "-downloadWindow=2m",
			},
			want: etlFlags{
				kind:           kindExpenses,
				initDate:       time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
				endDate:        time.Date(2025, 1, 31, 0, 0, 0, 0, time.UTC),
				codes:          []int64{26421, 26415},
				byManagingCode: true,
				trigger:        triggerScheduled,
				logLevel:       logger.LevelDebug,
				logLevelName:   "debug",
				concurrency:    2,
				debug:          true,
				force:          true,
				download:       portal.DownloadOptions{Limit: 50, Window: 2 * time.Minute},
			},
		},
		{
			name: "budget with subordinate agency codes",
			args: []string{"-kind=budget", "-init=2025-01-01", "-end=2026-12-31", "-codes=26421,26415"},
			want: etlFlags{
				kind:         kindBudget,
				initDate:     time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
				endDate:      time.Date(2026, 12, 31, 0, 0, 0, 0, time.UTC),
				codes:        []int64{26421, 26415},
				trigger:      triggerManual,
				logLevel:     logger.LevelInfo,
				logLevelName: "info",
				concurrency:  10,
				download:     portal.DefaultDownloadOptions(),
			},
		},
		{name: "budget without codes", args: []string{"-kind=budget"}, wantErr: "-kind=budget requires -codes"},
		{name: "budget with a management unit code", args: []string{"-kind=budget", "-codes=26421,158454"}, wantErr: `got 158454`},
		{
			name: "other kinds still take management unit codes",
			args: []string{"-kind=expenses", "-codes=158454"},
			want: etlFlags{
				kind:         kindExpenses,
				initDate:     yesterday,
				endDate:      yesterday,
				codes:        []int64{158454},
				trigger:      triggerManual,
				logLevel:     logger.LevelInfo,
				logLevelName: "info",
				concurrency:  10,
				download:     portal.DefaultDownloadOptions(),
			},
		},
		{
			name: "values are case-insensitive and codes are trimmed",
			args: []string{"-kind=BUDGET", "-trigger=manual", "-loglevel=WARN", "-codes= 26421 , 26415,"},
			want: etlFlags{
				kind:         kindBudget,
				initDate:     yesterday,
				endDate:      yesterday,
				codes:        []int64{26421, 26415},
				trigger:      triggerManual,
				logLevel:     logger.LevelWarn,
				logLevelName: "warn",
				concurrency:  10,
				download:     portal.DefaultDownloadOptions(),
			},
		},
		{name: "unknown kind", args: []string{"-kind=payments"}, wantErr: `invalid -kind "payments"`},
		{name: "bad init date", args: []string{"-init=2025/01/01"}, wantErr: `invalid -init "2025/01/01"`},
		{name: "bad end date", args: []string{"-end=31-01-2025"}, wantErr: `invalid -end "31-01-2025"`},
		{name: "end before init", args: []string{"-init=2025-02-01", "-end=2025-01-31"}, wantErr: "-end (2025-01-31) is before -init (2025-02-01)"},
		{name: "non-numeric code", args: []string{"-codes=26421,abc"}, wantErr: `invalid code "abc" in -codes`},
		{name: "no codes", args: []string{"-codes= , "}, wantErr: "-codes must contain at least one code"},
		{name: "unknown trigger", args: []string{"-trigger=CRON"}, wantErr: `invalid -trigger "CRON"`},
		{name: "unknown log level", args: []string{"-loglevel=verbose"}, wantErr: `invalid -loglevel "verbose"`},
		{name: "zero concurrency", args: []string{"-concurrency=0"}, wantErr: "-concurrency must be at least 1, got 0"},
		{name: "zero download limit", args: []string{"-downloadLimit=0"}, wantErr: "-downloadLimit must be at least 1, got 0"},
		{name: "zero download window", args: []string{"-downloadWindow=0s"}, wantErr: "-downloadWindow must be positive, got 0s"},
		{name: "bad download window", args: []string{"-downloadWindow=5"}, wantErr: `invalid value "5" for flag -downloadWindow`},
		{name: "unknown flag", args: []string{"-foo"}, wantErr: "flag provided but not defined: -foo"},
		{name: "positional argument", args: []string{"expenses"}, wantErr: `unexpected argument "expenses"`},
		{
			name:    "reports every invalid flag at once",
			args:    []string{"-kind=x", "-concurrency=-1"},
			wantErr: `invalid -kind "x"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseFlags(tt.args, now, io.Discard)
			if tt.wantErr != "" {
				if err == nil {
					t.Fatalf("expected error containing %q, got nil", tt.wantErr)
				}
				if !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("expected error containing %q, got %q", tt.wantErr, err.Error())
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("got  %+v\nwant %+v", got, tt.want)
			}
		})
	}
}

func TestParseFlagsReportsAllErrors(t *testing.T) {
	_, err := parseFlags([]string{"-kind=x", "-concurrency=-1", "-trigger=CRON"}, time.Now(), io.Discard)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	for _, want := range []string{"-kind", "-concurrency", "-trigger"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not mention %s", err.Error(), want)
		}
	}
}

func TestParseFlagsHelp(t *testing.T) {
	var out strings.Builder
	_, err := parseFlags([]string{"-h"}, time.Now(), &out)
	if !errors.Is(err, flag.ErrHelp) {
		t.Fatalf("expected flag.ErrHelp, got %v", err)
	}
	for _, want := range []string{"Usage:", "-kind", "Examples:"} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("usage output missing %q:\n%s", want, out.String())
		}
	}
}
