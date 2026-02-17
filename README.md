# Transparency Wrapper (Envelopa Transparência)

## Overview

**Transparency Wrapper** is a Go-based ETL (Extract, Transform, Load) tool and API designed to streamline the access and processing of Brazilian public transparency data. 

The primary goal of this project is to "wrap" data from the Brazilian Transparency Portal, making it agile and efficient for public management. By automating the bureaucratic and often slow process of manual consultation, it enables quick search and data handling for better decision-making.

## Core Flow (ETL)

The project follows a structured data pipeline:

1.  **Data Capture (Download):** Automatically fetches compressed data (ZIP files) from the Brazilian Transparency Portal for specific dates.
2.  **Extraction (Unzip):** Extracts raw CSV files from the downloaded archives into temporary storage.
3.  **Treatment (Transform):** 
    *   Parses and cleans raw CSV data using high-performance dataframes.
    *   Filters information based on specific Management Units (UG) or Management Codes.
    *   Organizes data into a structured hierarchical format (Commitments, Liquidations, Payments).
4.  **Ingestion (Load):** Ingests the treated and structured data into a PostgreSQL database for persistent storage and optimized querying.

## Project Structure

The codebase is organized into a clean and modular architecture:

*   `cmd/`: Entry points for the application.
    *   `api/`: The REST API server for querying ingested data.
    *   `etl/`: The main ETL process runner.
    *   `migrate/`: Database migration tools and SQL scripts.
*   `internal/`: Core business logic and private packages.
    *   `transparency/`: Orchestrates the ETL logic (Downloader, Assembler, Converter, and Query engine).
    *   `store/`: Data access layer, including database models and repository implementations.
    *   `db/`: Database connection and configuration.
    *   `logger/`: Structured logging for system monitoring.
    *   `env/`: Environment variable and configuration management.
*   `output/`: Stores processed extraction results in JSON format.
*   `tmp/`: Temporary workspace for downloaded ZIP files and extracted CSVs.

## API Endpoints

Once data is ingested, the system provides a specialized API to access processed information:

### Documentation
*   `GET /v1/docs/*`: Interactive Swagger UI documentation for all API endpoints.

### Health
*   `GET /v1/health`: Checks system health and database connectivity.

### Expenses
*   `GET /v1/expenses/summary`: Returns a summary of expenses by management units.
    *   **Parameters:** `management_code` (required), `management_unit_codes`, `start_date`, `end_date`
*   `GET /v1/expenses/summary/by-management`: Returns a global summary of expenses by management code.
    *   **Parameters:** `management_code` (required), `start_date`, `end_date`
*   `GET /v1/expenses/budget-execution/report`: Generates detailed budget execution reports.
    *   **Parameters:** `management_code` (required), `management_unit_codes`, `start_date`, `end_date`
*   `GET /v1/expenses/top-favored`: Returns the top favored entities (suppliers/contractors) based on payment values.
    *   **Parameters:** `management_code` (required), `management_unit_codes`, `start_date`, `end_date`, `limit` (default: 10)

### Commitments
*   `GET /v1/commitments/`: Retrieves detailed commitment information with filtering options.
    *   **Parameters:** `start_date`, `end_date`, `management_code`, `management_unit_codes`, `commitment_codes`

### Ingestion
*   `GET /v1/ingestion/history`: Returns the history of data ingestion processes.
    *   **Parameters:** `limit` (default: 10)
*   `POST /v1/ingestion`: Creates a new ingestion record to track ETL execution.

## Technologies Used

*   **Language:** Go (Golang)
*   **Database:** PostgreSQL
*   **Routing:** Chi Router
*   **Documentation:** Swagger/OpenAPI
*   **Data Processing:** Gota (Dataframes for Go)
*   **Containerization:** Docker & Docker Compose
*   **Migrations:** Standard SQL migrations

## Getting Started

### Prerequisites

*   Go 1.24+
*   Docker & Docker Compose
*   PostgreSQL

### Setup

1.  **Clone the repository:**
    ```bash
    git clone https://github.com/farxc/envelopa-transparencia.git
    cd envelopa-transparencia
    ```

2.  **Start the database:**
    ```bash
    docker-compose up -d
    ```

3.  **Run migrations:**
    ```bash
    go run cmd/migrate/main.go
    ```

### Running the ETL Process

The ETL tool is highly flexible and allows you to extract data for specific periods and organizations. You can filter data by **Management Unit (Unidade Gestora)** or **Management Code (Código de Gestão)**.

#### ETL CLI Flags:
*   `-init`: Initial date for extraction (Format: `YYYY-MM-DD`). Defaults to yesterday.
*   `-end`: End date for extraction (Format: `YYYY-MM-DD`). Defaults to yesterday.
*   `-codes`: Comma-separated list of numeric codes to filter.
*   `-byManagingCode`: Boolean flag. If `true`, the system filters by **Management Code**. If `false` (default), it filters by **Management Unit (UG) Code**.
*   `-loglevel`: Controls logging verbosity (`debug`, `info`, `warn`, `error`). Defaults to `info`.

#### Examples:

**1. Extracting by Management Unit (Default):**
```bash
go run cmd/etl/main.go -init 2025-01-01 -end 2025-01-05 -codes "158454,158148"
```

**2. Extracting by Management Code:**
```bash
go run cmd/etl/main.go -init 2025-01-01 -end 2025-01-05 -byManagingCode -codes "158454,158148"
```


### Running the API process
**Start the API:**
```bash
air
```

**Access API Documentation:**
Once the API is running, you can access the interactive Swagger documentation at:
    ```
    http://localhost:8080/v1/docs/index.html
    ```
