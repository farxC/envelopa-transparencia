package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/farxc/envelopa-transparencia/internal/infrastructure/client/portal"
	"github.com/farxc/envelopa-transparencia/internal/infrastructure/logger"
)

const (
	kindExpenses          = "expenses"
	kindExpensesExecution = "expenses_execution"
	kindBudget            = "budget"

	triggerManual    = "MANUAL"
	triggerScheduled = "SCHEDULED"
)

var (
	validKinds    = []string{kindExpenses, kindExpensesExecution, kindBudget}
	validTriggers = []string{triggerManual, triggerScheduled}
	logLevels     = map[string]logger.LogLevel{
		"debug": logger.LevelDebug,
		"info":  logger.LevelInfo,
		"warn":  logger.LevelWarn,
		"error": logger.LevelError,
	}
	// IFRO management unit codes extracted when -codes is not given.
	defaultCodes = []int64{158454, 158148, 158341, 158342, 158343, 158345, 158376, 158332, 158533, 158635, 158636}
)

// etlFlags holds the validated command-line configuration of the ETL.
type etlFlags struct {
	kind           string
	initDate       time.Time
	endDate        time.Time
	codes          []int64
	byManagingCode bool
	trigger        string
	logLevel       logger.LogLevel
	logLevelName   string
	concurrency    int
	debug          bool
	force          bool
	download       portal.DownloadOptions
}

const usageExamples = `
Examples:
  # Daily expenses for January 2025, filtering by management code
  etl -kind=expenses -init=2025-01-01 -end=2025-01-31 -codes=26421,26415 -byManagingCode -concurrency=2

  # Monthly budget execution for 2025
  etl -kind=expenses_execution -init=2025-01-01 -end=2025-12-31 -codes=26421,26415 -byManagingCode

  # Reload days already in the ingestion history, reusing cached ZIPs
  etl -kind=expenses -init=2025-02-17 -end=2026-09-30 -codes=26421,26415 -byManagingCode -force -concurrency=2

  # Yearly budget for 2025 and 2026
  etl -kind=budget -init=2025-01-01 -end=2026-12-31 -codes=26421,26415
`

// parseFlags parses and validates args (without the program name). now is used
// to compute the default dates. Usage and parse errors are written to output.
// Every invalid value is reported, not just the first one. It returns
// flag.ErrHelp when -h or -help is given.
func parseFlags(args []string, now time.Time, output io.Writer) (etlFlags, error) {
	fs := flag.NewFlagSet("etl", flag.ContinueOnError)
	fs.SetOutput(output)
	fs.Usage = func() {
		fmt.Fprintf(output, "Usage: etl [flags]\n\nExtracts data from the Transparency Portal and loads it into the database.\n\nFlags:\n")
		fs.PrintDefaults()
		fmt.Fprint(output, usageExamples)
	}

	yesterday := now.AddDate(0, 0, -1).Format(time.DateOnly)
	kind := fs.String("kind", kindExpensesExecution, "kind of data to extract: "+strings.Join(validKinds, ", "))
	initDate := fs.String("init", yesterday, "first date to extract, `YYYY-MM-DD`")
	endDate := fs.String("end", yesterday, "last date to extract, inclusive, `YYYY-MM-DD`")
	codes := fs.String("codes", joinCodes(defaultCodes), "comma-separated management unit (or management) `codes`; budget requires subordinate agency codes (e.g. 26421,26415)")
	byManagingCode := fs.Bool("byManagingCode", false, "match codes against \"Código Gestão\" instead of \"Código Unidade Gestora\" (ignored by budget, which always matches \"CÓDIGO ÓRGÃO SUBORDINADO\")")
	trigger := fs.String("trigger", triggerManual, "trigger recorded in the ingestion history: "+strings.Join(validTriggers, ", "))
	logLevel := fs.String("loglevel", "info", "log `level`: debug, info, warn, error")
	concurrency := fs.Int("concurrency", 10, "number of concurrent `workers`; keep it low for long ranges to avoid the portal's rate limit")
	debug := fs.Bool("debug", false, "save matched dataframes to CSV and bypass ingestion history checks")
	force := fs.Bool("force", false, "reprocess jobs already recorded as SUCCESS or SKIPPED in the ingestion history (jobs still IN_PROGRESS are skipped); cached ZIPs are reused")
	defaultDownload := portal.DefaultDownloadOptions()
	downloadLimit := fs.Int("downloadLimit", defaultDownload.Limit, "maximum portal downloads per -downloadWindow, for this process only; the portal blocks above roughly 80-100 per 5 minutes per IP, so split it between ETL runs that download at the same time")
	downloadWindow := fs.Duration("downloadWindow", defaultDownload.Window, "sliding window for -downloadLimit, and how long downloads pause when the portal blocks them")

	if err := fs.Parse(args); err != nil {
		return etlFlags{}, err
	}

	f := etlFlags{
		kind:           strings.ToLower(strings.TrimSpace(*kind)),
		byManagingCode: *byManagingCode,
		trigger:        strings.ToUpper(strings.TrimSpace(*trigger)),
		logLevelName:   strings.ToLower(strings.TrimSpace(*logLevel)),
		concurrency:    *concurrency,
		debug:          *debug,
		force:          *force,
		download:       portal.DownloadOptions{Limit: *downloadLimit, Window: *downloadWindow},
	}

	var errs []error
	if fs.NArg() > 0 {
		errs = append(errs, fmt.Errorf("unexpected argument %q (flags must be written as -name=value)", fs.Arg(0)))
	}
	if !slices.Contains(validKinds, f.kind) {
		errs = append(errs, fmt.Errorf("invalid -kind %q: must be one of %s", *kind, strings.Join(validKinds, ", ")))
	}

	var err error
	initOK, endOK := true, true
	if f.initDate, err = time.Parse(time.DateOnly, *initDate); err != nil {
		initOK = false
		errs = append(errs, fmt.Errorf("invalid -init %q: expected YYYY-MM-DD", *initDate))
	}
	if f.endDate, err = time.Parse(time.DateOnly, *endDate); err != nil {
		endOK = false
		errs = append(errs, fmt.Errorf("invalid -end %q: expected YYYY-MM-DD", *endDate))
	}
	if initOK && endOK && f.endDate.Before(f.initDate) {
		errs = append(errs, fmt.Errorf("-end (%s) is before -init (%s)", *endDate, *initDate))
	}

	if f.codes, err = parseCodes(*codes); err != nil {
		errs = append(errs, err)
	} else if f.kind == kindBudget {
		errs = append(errs, validateBudgetCodes(fs, f.codes)...)
	}
	if !slices.Contains(validTriggers, f.trigger) {
		errs = append(errs, fmt.Errorf("invalid -trigger %q: must be one of %s", *trigger, strings.Join(validTriggers, ", ")))
	}

	level, ok := logLevels[f.logLevelName]
	if !ok {
		errs = append(errs, fmt.Errorf("invalid -loglevel %q: must be one of debug, info, warn, error", *logLevel))
	}
	f.logLevel = level

	if f.concurrency < 1 {
		errs = append(errs, fmt.Errorf("-concurrency must be at least 1, got %d", f.concurrency))
	}

	if f.download.Limit < 1 {
		errs = append(errs, fmt.Errorf("-downloadLimit must be at least 1, got %d", f.download.Limit))
	}
	if f.download.Window <= 0 {
		errs = append(errs, fmt.Errorf("-downloadWindow must be positive, got %s", f.download.Window))
	}

	if len(errs) > 0 {
		return etlFlags{}, errors.Join(errs...)
	}
	return f, nil
}

// validateBudgetCodes checks the codes of a budget run. The budget file is
// matched by "CÓDIGO ÓRGÃO SUBORDINADO", a 5-digit code; the default -codes are
// management unit codes, which would match nothing and mark the year SKIPPED.
func validateBudgetCodes(fs *flag.FlagSet, codes []int64) []error {
	codesSet := false
	fs.Visit(func(fl *flag.Flag) {
		if fl.Name == "codes" {
			codesSet = true
		}
	})
	if !codesSet {
		return []error{errors.New("-kind=budget requires -codes with subordinate agency codes (5 digits, e.g. -codes=26421,26415)")}
	}
	var errs []error
	for _, c := range codes {
		if c < 10000 || c > 99999 {
			errs = append(errs, fmt.Errorf("-kind=budget filters by \"CÓDIGO ÓRGÃO SUBORDINADO\" (5 digits, e.g. 26421); got %d", c))
		}
	}
	return errs
}

// parseCodes parses a comma-separated list of numeric codes, ignoring blanks.
func parseCodes(raw string) ([]int64, error) {
	var codes []int64
	for part := range strings.SplitSeq(raw, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		code, err := strconv.ParseInt(part, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid code %q in -codes: must be numeric", part)
		}
		codes = append(codes, code)
	}
	if len(codes) == 0 {
		return nil, errors.New("-codes must contain at least one code")
	}
	return codes, nil
}

func joinCodes(codes []int64) string {
	parts := make([]string, len(codes))
	for i, c := range codes {
		parts[i] = strconv.FormatInt(c, 10)
	}
	return strings.Join(parts, ",")
}
