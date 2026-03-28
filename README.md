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

### Documentation
*   `GET /v1/docs/*`: Interactive Swagger UI documentation.

### Expenses
*   `GET /v1/expenses/summary`: Summary by management units.
*   `GET /v1/expenses/summary/by-management`: Global summary by management code.
*   `GET /v1/expenses/budget-execution/report`: Detailed budget execution reports.
*   `GET /v1/expenses/top-favored`: Top favored entities (suppliers/contractors).

### Commitments
*   `GET /v1/commitments/`: Detailed commitment information with filtering.


### Ingestion
*   `GET /v1/ingestion/history`: History of data ingestion processes.
*   `POST /v1/ingestion`: Manual creation of ingestion records.

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

### Setup
1.  `dockercompose up --build`
2.  `make migrate-up`

### Running the ETL Process
```bash
go run cmd/etl/main.go -init 2025-01-01 -end 2026-03-22 -byManagingCode=true -codes='26421,26415'
```

### Running the API
```bash
go run cmd/api/main.go # or 'air' for hot reload
```
