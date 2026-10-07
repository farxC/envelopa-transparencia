# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Service Does

ETL + REST API that crawls the Brazilian Transparency Portal (Portal da Transparência), downloads ZIP files containing CSV data, filters by IFRO management unit codes, transforms the data, and persists only the relevant records. Exposes the stored data via a REST API consumed by `SAGA_IFRO_API`.

## Commands

```bash
# API server (run the package, not main.go alone — main.go needs api.go etc.)
go run ./cmd/api

# ETL via Make — one target per kind; INIT, END, CODES, BY_MANAGING_CODE,
# CONCURRENCY, LOGLEVEL, TRIGGER, DEBUG, FORCE, DOWNLOAD_LIMIT, DOWNLOAD_WINDOW are
# optional and map to the flags below
make etl-expenses INIT=2025-01-01 END=2025-01-31 CODES=26421,26415 BY_MANAGING_CODE=true CONCURRENCY=2
make etl-expenses-execution INIT=2025-01-01 END=2025-12-31 CODES=26421,26415 BY_MANAGING_CODE=true
make etl-budget INIT=2025-01-01 END=2026-12-31 CODES=26421,26415

# ETL directly (all flags shown with defaults)
go run ./cmd/etl \
  -kind=expenses_execution \   # expenses | expenses_execution | budget
  -init=<yesterday> \
  -end=<yesterday> \
  -codes='158454,158148,...' \ # comma-separated unit/management codes
  -byManagingCode=false \      # true = filter by management code column
  -concurrency=10 \
  -loglevel=info \             # debug, info, warn, error
  -trigger=MANUAL \            # MANUAL or SCHEDULED
  -debug=false \               # true = save filtered CSVs, bypass history checks
  -force=false \               # true = reprocess SUCCESS/SKIPPED jobs (not fresh IN_PROGRESS), reusing cached ZIPs
  -downloadLimit=70 \          # max portal downloads per window (portal blocked at 82-103/5min)
  -downloadWindow=5m1s         # sliding window for -downloadLimit; pause length after a block

# Migrations
make migrate-up
make migrate-down N
make migration NAME            # create new migration pair

# Swagger docs regeneration
make gen-docs

# Tests
go test ./...
go test ./internal/utils/...   # run a specific package
```

## Architecture

### DDD Layers

```
cmd/api/          → Chi HTTP server (routes, handlers, middleware)
cmd/etl/          → CLI orchestrator entry point
cmd/migrate/      → Migration runner + SQL files

internal/domain/
  model/          → Entities (pure structs, no DB tags)
  repository/     → Repository interfaces
  service/        → DTOs, gateway/loader interfaces, assembler logic

internal/application/
  orchestrator.go                      → Generic Orchestrator[J] with worker pool, retries, idempotency
  pipeline_expenses_daily.go           → Pipeline[ExpensesDailyJob]
  pipeline_expenses_execution.go       → Pipeline[ExpensesExecutionJob]
  pipeline_budget.go                   → Pipeline[BudgetJob]

internal/infrastructure/
  client/portal/  → HTTP downloader (portal.go), DataFrame queries (query.go), CSV mappers (mapper.go)
  store/          → sqlx repository implementations + transactional loader
  filesystem/     → ZIP extraction, CSV reading (encoding-aware)
  db/             → sqlx connection pool
  env/            → Load (.env via godotenv) + GetString/GetInt env helpers
  logger/         → Leveled structured logger

internal/utils/
  parser.go       → ParseFloat (pt-BR format), ParseDate, ParseInt64, ParseBool
```

### Three Extraction Types

| Kind | Granularity | Source | Data |
|------|-------------|--------|------|
| `expenses` | Per day | `despesas/YYYYMMDD` | Commitment → Liquidation → Payment full lifecycle |
| `expenses_execution` | Per month | `despesas-execucao/YYYYMM` | Monthly aggregated budget execution rows |
| `budget` | Per year | `orcamento-despesa/YYYY` | Yearly expense budget (ignores `-byManagingCode`) |

Downloaded ZIPs are cached in `tmp/zips/<kind>/`; `expenses` reuses a cached ZIP, `expenses_execution` and `budget` always re-download.

### ETL Pipeline Steps (all kinds follow the same contract)

1. **Download** — `portal.go` fetches ZIP from transparency portal (User-Agent spoofed)
2. **Extract** — `filesystem/local.go` unzips to `tmp/data/`; skips irrelevant files (Bancos, Faturas, Precatorios)
3. **Filter** — `client/portal/query.go` uses Gota DataFrames to match rows by `Código Gestão` or `Código Unidade Gestora`
4. **Map** — `client/portal/mapper.go` converts raw CSV rows to domain entities; parsers handle pt-BR float format (e.g. `1.234,56`)
5. **Assemble** — `domain/service/assembler.go` groups flat entities into hierarchies (items under commitments, impacts under payments)
6. **Load** — `infrastructure/store/loader.go` runs per-unit DB transactions with upserts (`ON CONFLICT DO UPDATE`) and orphan cleanup (DELETE children, then INSERT)

### Orchestrator Pattern

`Orchestrator[J any]` in `internal/application/orchestrator.go`:
- Worker pool (configurable concurrency, default 10)
- Loads `IngestionHistory` at startup to skip already-processed jobs (`ShouldProcess`)
- Creates `IN_PROGRESS` record before processing; updates to `SUCCESS`/`FAILURE`/`SKIPPED` after
- Up to 3 retries per job; stale-timeout at 30 minutes
- Extending to a new extraction type: implement the `Pipeline[J]` interface (5 methods: `Execute`, `BuildHistoryRecord`, `ShouldSkip`, `StatusKey`, `HistoryRange`)

### Domain Model Key Entities

- **Commitment** (`empenho`) — has `[]CommitmentItem`, each with `[]CommitmentItemsHistory`
- **Liquidation** — has `[]LiquidationImpactedCommitment` (links to commitment codes)
- **Payment** — has `[]PaymentImpactedCommitment`
- **ExpenseExecution** — monthly aggregate (committed/liquidated/paid values per budget line)
- **IngestionHistory** — audit record per job (status, codes processed, trigger type)

### API Endpoints

Base: `/v1/`

| Method | Path | Handler |
|--------|------|---------|
| GET | `/expenses/summary` | Summary by management unit |
| GET | `/expenses/summary/by-management` | Global summary |
| GET | `/expenses/budget-execution/report` | Budget report by expense nature |
| GET | `/expenses/top-favored` | Top suppliers by payment value |
| GET | `/budget-execution/` | Monthly execution data |
| GET | `/budget/` | Expense budget records |
| GET | `/budget/summary` | Expense budget summary |
| GET | `/budget/global-summary` | Global expense budget summary |
| GET | `/commitments/` | Commitments with items & history |
| GET | `/ingestion/history` | Audit records |
| POST | `/ingestion` | Create ingestion record |
| GET | `/health` | Health check |

Most queries require `management_code`; optionally accept `management_unit_codes`, `start_date`, `end_date` (YYYY-MM-DD).

## Configuration

`cmd/api` and `cmd/etl` call `env.Load()` first thing, which loads `.env` from the working directory via godotenv (already-set variables win; a missing `.env` is ignored). The Makefile also `include`s `.env`. Run commands from the repo root.

- `.env` — host development: `DB_ADDR` points at `localhost:5454`
- `.env.production` — Docker stack (`docker-compose.prod.yml`): `DB_ADDR` points at `db:5432`

Key environment variables:

```
DB_ADDR               # full postgres connection string
DB_MAX_OPEN_CONNS     # default 25
DB_MAX_IDLE_CONNS     # default 25
DB_MAX_IDLE_TIME      # default 15m
ADDR                  # API listen address (default :8080)
```

## Testing

Table-driven unit tests exist for pt-BR number/date parsing (`internal/utils/parser_test.go`), ETL flag parsing (`cmd/etl/flags_test.go`) and the rate-limited downloader (`internal/infrastructure/client/portal/download_test.go`, uses `httptest`). There are no DB integration tests; infrastructure is tested manually via Docker.

```bash
go test ./internal/utils/ ./cmd/etl/ ./internal/infrastructure/client/portal/
```

## Important Quirks

- **pt-BR float format**: `utils/parser.go:ParseFloat` handles both `1.234,56` and `1234.56` — always use this, never `strconv.ParseFloat` directly on portal data.
- **Idempotency relies on**: (a) unique DB constraints on `commitment_code`, `payment_code`, `liquidation_code`; (b) orchestrator status map from `IngestionHistory`. The ETL is safe to re-run.
- **ETL flags are validated up front** (`cmd/etl/flags.go:parseFlags`), before the DB connection: unknown `-kind`/`-trigger`/`-loglevel`, malformed dates, `-end` before `-init`, non-numeric codes and `-concurrency < 1` all exit with code 2 and list every problem. `-kind`, `-trigger` and `-loglevel` are case-insensitive; blank entries in `-codes` are ignored. Add new flags there (with a test case), not in `main.go`.
- **Payment → commitment links** (`payment_impacted_commitments`) are matched by `Código Pagamento` of the day's payments and attached to `Payment.ImpactedCommitments`, like liquidations. Never match them by the day's commitments: a payment almost always settles a commitment issued on an earlier day, so that drops ~96% of the links.
- **`budget` codes are subordinate agency codes** (`CÓDIGO ÓRGÃO SUBORDINADO`, 5 digits: 26421 IFRO, 26415 IFMS), not management unit codes. `parseFlags` requires an explicit `-codes` for `-kind=budget` and rejects other lengths — the default unit codes would match nothing and mark the year `SKIPPED`. The budget file has no management unit, so there is no per-campus budget.
- **`budget` load**: rows sharing the `expense_budget` key are summed (`service.AggregateBudgetRows`), `percent_executed_budget` is computed (`service.BudgetPercentExecuted`), not parsed, and each year is loaded in one transaction that first deletes that year's rows for the requested agencies. The portal regenerates the yearly file daily, so every budget run downloads it again (never the cached ZIP; a failed download fails the job) and reloads years already in the history as `SUCCESS` — only a year `IN_PROGRESS` in another run is skipped. Each run costs one download per year in `-init`..`-end`.
- **`-force=true`**: reprocesses days/months/years already marked `SUCCESS`/`SKIPPED`, skipping only jobs `IN_PROGRESS` within the 30-minute stale timeout. Cached ZIPs are reused, but days without a cached ZIP are still downloaded — check `tmp/zips/<kind>/` first if the portal budget matters.
- **`-debug=true`**: saves filtered DataFrames to CSV and bypasses `IngestionHistory` checks — useful for investigating raw portal data without polluting the history table.
- **Tmp dirs**: ETL creates `tmp/zips/` and `tmp/data/` under the working directory at startup. Already-downloaded `expenses` ZIPs are reused (checked via `os.Stat`); `budget` is always downloaded again, since the portal regenerates the yearly file daily.
- **DB connection fails fast**: `db.New` pings the database and returns the error, so bad credentials surface at startup (`pq: password authentication failed`). If that happens on the host, `DB_ADDR` is missing or the password doesn't match the Postgres volume — `docker-compose.yml` uses `helloworld`, the prod compose uses `.env.production`; both publish port 5454.
- **Compose `$$DB_ADDR`**: the `migrate` service command in `docker-compose.prod.yml` must use `$$DB_ADDR`. A single `$` is interpolated by Compose from the host `.env` (which points at `localhost`), not from the service's `env_file`.
- **Portal rate limiting**: the download host `dadosabertos-download.cgu.gov.br` sits behind AWS WAF and answers with a CAPTCHA (`405` + `x-amzn-waf-action: captcha`) above roughly 80–100 requests per 5 minutes (observed blocks at 82 and 103), for every kind. `client/portal/download.go` handles it: a sliding-window limiter shared by all workers (`-downloadLimit`, default 70, per `-downloadWindow`, default 5m1s — exactly 5m was measured to be too short) gates every download, and a WAF block pauses all downloads for one window and retries the file in the client (max 3) instead of failing the job. All `Fetch*` methods must go through `download()`. Downloads are written to `<path>.part` and renamed, so a cached ZIP is never partial. Don't try to bypass the CAPTCHA.
- **The download limit is per process, the portal's is per public IP**: the limiter is in memory, so concurrent ETL processes (two kinds in parallel, scheduled + manual, host + Docker, other machines on the same network) each get the full budget, and a process started right after another starts with an empty counter. Any of these can exceed the portal threshold and trigger the block. Run downloading ETLs one at a time, wait one window between heavy runs, or split `-downloadLimit` between processes. Nothing enforces this — if you add scheduling or parallel runs, add a cross-process limit (e.g. a DB-backed counter or advisory lock) first.
- **`expenses` vs `expenses_execution`** are entirely separate data sources with separate DB tables and separate pipelines — don't conflate them.
