include .env

MIGRATIONS_PATH = ./cmd/migrate/migrations


.PHONY: migrate-create
migration:
	@migrate create -seq -ext sql -dir $(MIGRATIONS_PATH) $(filter-out $@,$(MAKECMDGOALS))

.PHONY: migrate-up
migrate-up:
	@migrate -path=$(MIGRATIONS_PATH) -database=$(DB_ADDR) up

.PHONY: migrate-down
migrate-down:
	@migrate -path=$(MIGRATIONS_PATH) -database=$(DB_ADDR) down $(filter-out $@,$(MAKECMDGOALS))


# ----------------------------------------------------------------------------
# ETL
# ----------------------------------------------------------------------------
# Optional variables (unset ones fall back to the defaults in cmd/etl/main.go):
#   INIT=YYYY-MM-DD          start date                    (default: yesterday)
#   END=YYYY-MM-DD           end date, inclusive           (default: yesterday)
#   CODES=26421,26415        comma-separated unit/management codes
#   BY_MANAGING_CODE=true    match "Código Gestão" instead of "Código Unidade Gestora"
#   CONCURRENCY=2            parallel workers              (default: 10)
#   LOGLEVEL=debug           debug | info | warn | error   (default: info)
#   TRIGGER=SCHEDULED        MANUAL | SCHEDULED            (default: MANUAL)
#   DEBUG=true               save filtered CSVs and ignore ingestion history
#   FORCE=true               reprocess jobs already SUCCESS/SKIPPED in the ingestion history
#   DOWNLOAD_LIMIT=70        max portal downloads per DOWNLOAD_WINDOW (default: 70)
#   DOWNLOAD_WINDOW=5m1s     sliding window for DOWNLOAD_LIMIT (default: 5m1s)
#
# Examples:
#   make etl-expenses INIT=2025-01-01 END=2025-01-31 CODES=26421,26415 BY_MANAGING_CODE=true CONCURRENCY=2
#   make etl-expenses-execution INIT=2025-01-01 END=2025-12-31 CODES=26421,26415 BY_MANAGING_CODE=true
#   make etl-budget INIT=2025-01-01 END=2026-12-31 CODES=26421,26415
#   make etl-expenses INIT=2025-02-17 END=2025-02-17 CODES=26421 DEBUG=true LOGLEVEL=debug
ETL_FLAGS = $(if $(INIT),-init=$(INIT)) \
	$(if $(END),-end=$(END)) \
	$(if $(CODES),-codes='$(CODES)') \
	$(if $(BY_MANAGING_CODE),-byManagingCode=$(BY_MANAGING_CODE)) \
	$(if $(CONCURRENCY),-concurrency=$(CONCURRENCY)) \
	$(if $(LOGLEVEL),-loglevel=$(LOGLEVEL)) \
	$(if $(TRIGGER),-trigger=$(TRIGGER)) \
	$(if $(DEBUG),-debug=$(DEBUG)) \
	$(if $(FORCE),-force=$(FORCE)) \
	$(if $(DOWNLOAD_LIMIT),-downloadLimit=$(DOWNLOAD_LIMIT)) \
	$(if $(DOWNLOAD_WINDOW),-downloadWindow=$(DOWNLOAD_WINDOW))

# Daily commitments, liquidations and payments (one ZIP per day).
.PHONY: etl-expenses
etl-expenses:
	@go run ./cmd/etl -kind=expenses $(ETL_FLAGS)

# Monthly aggregated budget execution (one ZIP per month).
.PHONY: etl-expenses-execution
etl-expenses-execution:
	@go run ./cmd/etl -kind=expenses_execution $(ETL_FLAGS)

# Yearly expense budget (one ZIP per year; BY_MANAGING_CODE is ignored).
.PHONY: etl-budget
etl-budget:
	@go run ./cmd/etl -kind=budget $(ETL_FLAGS)


.PHONY: gen-docs
gen-docs:
	@swag init -g ./cmd/api/main.go --parseDependency --parseInternal && swag fmt