# Transparency Wrapper (Envelopa Transparência)

## Overview

**Transparency Wrapper** is a Go-based ETL (Extract, Transform, Load) tool and API designed to streamline the access and processing of Brazilian public transparency data. 

The primary goal of this project is to "wrap" data from the Brazilian Transparency Portal, making it agile and efficient for public management. By automating the bureaucratic and often slow process of manual consultation, it enables quick search and data handling for better decision-making.

## Architecture (DDD Inspired)

The project follows a **Domain-Driven Design (DDD)** inspired architecture, ensuring strict separation between business rules and technical implementation details.

### 1. Domain Layer (`internal/domain`)
The heart of the application, containing the "bare" business logic and entities.
*   **Models:** Pure Go structs representing business concepts (Commitment, Liquidation, Payment).
*   **Repository Interfaces:** Definitions of how data should be stored and retrieved.
*   **Domain Services:** Pure business rules, such as the **Assembler**, which organizes flat entities into complex hierarchical structures.
*   **Gateway Interfaces:** Definitions for external interactions (e.g., the Transparency Portal client).

### 2. Application Layer (`internal/application`)
Coordinates the "Use Cases" of the system.
*   **Orchestrator:** Manages the ETL workflow (Sync state -> Fetch -> Assemble -> Load) without knowing technical details about databases or HTTP.

## Project Structure

The codebase follows a clean, layered architecture:

*   `cmd/`: Application entry points.
    *   `api/`: REST API server for querying transparency data.
    *   `etl/`: CLI tool to run the ETL pipeline.
    *   `migrate/`: Database migration utility and SQL scripts.
*   `internal/`: Private packages containing the core logic.
    *   `domain/`: The core business layer (Entities and Interfaces).
        *   `model/`: Domain entities (Commitment, Liquidation, Payment).
        *   `repository/`: Repository and Gateway interface definitions.
        *   `service/`: Business services (Assembler, DTO definitions).
    *   `application/`: Coordination layer.
        *   `orchestrator.go`: Manages the high-level ETL workflow.
    *   `infrastructure/`: External implementations and adapters.
        *   `client/portal/`: Transparency Portal scraper, query engine, and mappers (ACL).
        *   `store/`: PostgreSQL repository implementations and data loader.
        *   `db/`: Database connection pooling and configuration.
        *   `filesystem/`: Local file management (unzip, temp files).
        *   `env/`: Environment variable and config management.
        *   `logger/`: Structured logging system.
    *   `response/`: Standardized API response structures.
*   `output/`: Storage for processed extraction results (JSON).
*   `tmp/`: Temporary workspace for ZIP downloads and CSV extractions.

1.  **Orchestration:** The Application layer checks the `IngestionHistory` to see if a date needs processing.
2.  **Data Capture:** The Infrastructure Client fetches ZIP files from the Transparency Portal.
3.  **Anti-Corruption Layer (ACL):** Infrastructure **Mappers** translate raw CSV rows into Domain Models.
4.  **Domain Assembly:** The **Domain Assembler** service organizes these models into a structured hierarchy (Commitments with their respective Items and Liquidations).
5.  **Persistence:** The Infrastructure Store saves the final payload into PostgreSQL using atomic transactions.

---

## API Endpoints

Base path: `/v1`. Most queries require `management_code` and optionally accept `management_unit_codes`, `start_date` and `end_date` (`YYYY-MM-DD`).

### Documentation & Health
*   `GET /v1/docs/*`: Interactive Swagger UI documentation.
*   `GET /v1/health`: Health check.

### Expenses
*   `GET /v1/expenses/summary`: Summary by management units.
*   `GET /v1/expenses/summary/by-management`: Global summary by management code.
*   `GET /v1/expenses/budget-execution/report`: Detailed budget execution reports.
*   `GET /v1/expenses/top-favored`: Top favored entities (suppliers/contractors).

### Budget Execution
*   `GET /v1/budget-execution/`: Monthly budget execution data.

### Budget
*   `GET /v1/budget/`: Expense budget records.
*   `GET /v1/budget/summary`: Expense budget summary.
*   `GET /v1/budget/global-summary`: Global expense budget summary.

### Commitments
*   `GET /v1/commitments/`: Detailed commitment information with filtering.

### Ingestion
*   `GET /v1/ingestion/history`: History of data ingestion processes.
*   `POST /v1/ingestion`: Manual creation of ingestion records.

#### Examples
```bash
curl http://localhost:8080/v1/health
curl "http://localhost:8080/v1/expenses/summary?management_code=26421&start_date=2025-01-01&end_date=2025-01-31"
curl "http://localhost:8080/v1/ingestion/history"
```

---

## Technologies Used

*   **Language:** Go (Golang) 1.24+
*   **Architecture:** Domain-Driven Design (DDD)
*   **Database:** PostgreSQL (sqlx & golang-migrate)
*   **Routing:** Chi Router
*   **Data Processing:** Gota (Dataframes for Go)
*   **Containerization:** Docker & Docker Compose

---

## Getting Started

### Prerequisites
*   Go 1.24+
*   Docker & Docker Compose
*   [golang-migrate](https://github.com/golang-migrate/migrate) CLI (for `make migrate-*`)

### Configuration

Both binaries (`cmd/api` and `cmd/etl`) load `.env` from the working directory at startup, so run them from the repository root. Variables already set in your shell take precedence over `.env`. When no `.env` exists (e.g. inside the Docker image) the built-in defaults are used.

```dotenv
# .env — local development (host → Docker Postgres on port 5454)
POSTGRES_DB=transparency_wrapper_db
POSTGRES_USER=admin
POSTGRES_PASSWORD=<password>
DB_ADDR="postgres://admin:<password>@localhost:5454/transparency_wrapper_db?sslmode=disable"

DB_MAX_OPEN_CONNS=25
DB_MAX_IDLE_CONNS=25
DB_MAX_IDLE_TIME=15m
```

`.env.production` holds the same keys for the Docker stack, but `DB_ADDR` must point at the `db` service (`@db:5432`), since containers reach Postgres over the Docker network.

If the database is unreachable or the credentials are wrong, both binaries fail at startup with the Postgres error (e.g. `password authentication failed for user "admin"`).

### Setup

**Option A — full stack in Docker** (database + migrations + API, uses `.env.production`):
```bash
docker network create saga-shared   # once
docker compose -f docker-compose.prod.yml up -d --build
```

**Option B — database in Docker, API/ETL on the host** (uses `.env`):
```bash
docker compose up -d   # Postgres only, on localhost:5454
make migrate-up
```

The database password must match the one the Postgres volume was initialized with; `docker-compose.yml` uses `helloworld`, `docker-compose.prod.yml` uses `POSTGRES_PASSWORD` from `.env.production`. Both publish port 5454, so run only one at a time.

### Running the ETL

There are three extraction kinds, each with its own Make target:

| Target | Kind | One ZIP per | Data |
|---|---|---|---|
| `make etl-expenses` | `expenses` | day | Commitments, liquidations and payments |
| `make etl-expenses-execution` | `expenses_execution` | month | Aggregated budget execution |
| `make etl-budget` | `budget` | year | Expense budget |

Optional variables (unset ones fall back to the ETL defaults):

| Variable | ETL flag | Default | Description |
|---|---|---|---|
| `INIT` | `-init` | yesterday | Start date (`YYYY-MM-DD`) |
| `END` | `-end` | yesterday | End date, inclusive (`YYYY-MM-DD`) |
| `CODES` | `-codes` | IFRO unit codes | Comma-separated unit/management codes |
| `BY_MANAGING_CODE` | `-byManagingCode` | `false` | Match `Código Gestão` instead of `Código Unidade Gestora` |
| `CONCURRENCY` | `-concurrency` | `10` | Parallel workers |
| `LOGLEVEL` | `-loglevel` | `info` | `debug`, `info`, `warn`, `error` |
| `TRIGGER` | `-trigger` | `MANUAL` | `MANUAL` or `SCHEDULED` |
| `DEBUG` | `-debug` | `false` | Save filtered CSVs and ignore ingestion history |
| `DOWNLOAD_LIMIT` | `-downloadLimit` | `70` | Maximum portal downloads per `DOWNLOAD_WINDOW` |
| `DOWNLOAD_WINDOW` | `-downloadWindow` | `5m1s` | Sliding window for `DOWNLOAD_LIMIT`; also the pause after a block |

Examples:
```bash
# Daily expenses for January 2025, filtering by management code
make etl-expenses INIT=2025-01-01 END=2025-01-31 CODES=26421,26415 BY_MANAGING_CODE=true CONCURRENCY=2

# Monthly budget execution for all of 2025
make etl-expenses-execution INIT=2025-01-01 END=2025-12-31 CODES=26421,26415 BY_MANAGING_CODE=true

# Yearly budget for 2025 and 2026
make etl-budget INIT=2025-01-01 END=2026-12-31 CODES=26421,26415

# Inspect a single day's raw data without touching ingestion history
make etl-expenses INIT=2025-02-17 END=2025-02-17 CODES=26421 DEBUG=true LOGLEVEL=debug
```

The targets wrap `go run ./cmd/etl`, which can also be called directly:
```bash
go run ./cmd/etl -kind=expenses -init=2025-01-01 -end=2025-01-31 -byManagingCode=true -codes='26421,26415'
```

Run `go run ./cmd/etl -h` to see every flag with examples. Flags are validated before anything else runs: an unknown kind, a malformed date, `END` before `INIT`, a non-numeric code or `CONCURRENCY` below 1 stops the ETL immediately with a message listing every problem. `-kind`, `-trigger` and `-loglevel` are case-insensitive.

The ETL is idempotent: already-processed dates are skipped using the ingestion history, and `expenses`/`budget` ZIPs already in `tmp/zips/` are reused instead of downloaded again.

#### Portal rate limiting

The portal's download host (`dadosabertos-download.cgu.gov.br`) is protected by an AWS WAF rule that answers with a CAPTCHA (`405` + `x-amzn-waf-action: captcha`) once a client goes over roughly **80–100 requests in 5 minutes**. The threshold isn't exact. Measured on 2026-10-06:

- 103 downloads in 27 s went through and the 104th was blocked; the block lifted within ~6 minutes.
- In a later run, the portal blocked after only 82 downloads in 26 s.
- Pausing for exactly 5 minutes after a block wasn't enough: the next requests were blocked again, so the window must be slightly longer than 5 minutes.

The ETL paces itself so you don't have to:

- **Download limit:** at most `DOWNLOAD_LIMIT` downloads (default 70, below the lowest observed block point) start within any `DOWNLOAD_WINDOW` (default 5 minutes and 1 second), across all workers. Up to 70 uncached files download at full speed; longer backfills continue at about 14 per minute and log `Download throttled to stay under the portal's rate limit`.
- **Automatic pause:** if the portal blocks a request anyway, every download pauses for one window and the blocked file is retried (up to 3 times) without failing the job.
- **Only downloads are paced:** unzipping, filtering and loading keep running on all workers, and cached ZIPs never count against the limit.

A full-year `expenses` backfill (~365 uncached days) therefore takes roughly 25 minutes of downloading:

```bash
make etl-expenses INIT=2025-01-01 END=2025-12-31 CODES=26421,26415 BY_MANAGING_CODE=true
```

If the portal tightens its limit, lower the budget instead of changing code:

```bash
make etl-expenses INIT=2025-01-01 END=2025-12-31 CODES=26421,26415 DOWNLOAD_LIMIT=50 DOWNLOAD_WINDOW=5m1s
```

##### The limit is per process, the portal's is per IP

The download counter lives in the memory of a single ETL process. The portal, however, counts every request coming from your **public IP**. Each ETL process believes it has the full budget of 70 downloads, so the limiter does **not** protect you when:

| Situation | What the portal sees | How to avoid it |
|---|---|---|
| Two ETL runs at the same time (e.g. `expenses` and `budget` in two terminals, or a scheduled run overlapping a manual one) | Up to 2 × 70 requests in 5 minutes | Run kinds one after another, or split the budget: `DOWNLOAD_LIMIT=35` for each |
| Runs started back-to-back (one ends, the next starts immediately) | The new process starts with an empty counter, so up to 140 requests in 5 minutes | Wait `DOWNLOAD_WINDOW` (just over 5 minutes) between runs that download a lot |
| ETL on the host and in the Docker container at once, or on several machines behind the same network | All share one public IP | Only one ETL downloading per network at a time |
| Downloading from the portal manually (browser, `curl`) during a run | Those requests count too | Avoid it while the ETL is downloading |

If any of these happens, the automatic pause still recovers: the blocked downloads wait one window and retry. But every process pauses, the run takes longer, and repeated blocks fail the download after 3 retries. Make sure only one process downloads at a time; the ETL doesn't enforce this.

### Running the API
```bash
go run ./cmd/api   # or 'air' for hot reload
```

The API listens on `:8080` by default (override with `ADDR`, e.g. `ADDR=:18080 go run ./cmd/api`).

### Make targets

| Target | Description |
|---|---|
| `make migrate-up` | Apply all pending migrations |
| `make migrate-down N` | Roll back `N` migrations |
| `make migration NAME` | Create a new migration pair |
| `make gen-docs` | Regenerate Swagger docs |
| `make etl-expenses` | Run the daily expenses ETL |
| `make etl-expenses-execution` | Run the monthly budget execution ETL |
| `make etl-budget` | Run the yearly budget ETL |
