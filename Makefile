.PHONY: help up down reset seed backfill test bench status

help:
	@echo "Multi-Tenant Aggregation Layer - Available Targets:"
	@echo "  make up        - Start dual Postgres containers (ports 5433 & 5434) and wait for health"
	@echo "  make down      - Stop Postgres containers"
	@echo "  make reset     - Stop containers, delete database volumes, and recreate from scratch"
	@echo "  make status    - Check health status of database containers"
	@echo "  make seed      - Run synthetic data generator (e.g. make seed ROWS=1000000 TENANTS=100)"
	@echo "  make backfill  - Run worker backfill on existing source data"
	@echo "  make test      - Run full cargo test suite across all workspace crates"
	@echo "  make bench     - Run benchmark suite"

up:
	@echo "Starting dual PostgreSQL instances (Source: 5433, Analytics: 5434)..."
	docker compose up -d
	@echo "Waiting for PostgreSQL instances to become healthy..."
	@docker compose ps
	@until docker compose exec -T source_db pg_isready -U postgres -d source_db > /dev/null 2>&1; do \
		echo "Waiting for Source DB (5433)..."; \
		sleep 1; \
	done
	@until docker compose exec -T analytics_db pg_isready -U postgres -d analytics_db > /dev/null 2>&1; do \
		echo "Waiting for Analytics DB (5434)..."; \
		sleep 1; \
	done
	@echo "✅ Both PostgreSQL instances are healthy and ready!"

down:
	@echo "Stopping PostgreSQL instances..."
	docker compose down

reset:
	@echo "Resetting PostgreSQL instances and wiping all volumes..."
	docker compose down -v
	$(MAKE) up

status:
	@docker compose ps

seed:
	@echo "Running seed tool with ROWS=$${ROWS:-1000000} and TENANTS=$${TENANTS:-100}..."
	cargo run --bin seed -- --rows $${ROWS:-1000000} --tenants $${TENANTS:-100}

backfill:
	@echo "Running worker backfill..."
	cargo run --bin worker -- backfill

test:
	@echo "Running full workspace test suite..."
	cargo test --workspace

bench:
	@echo "Running benchmark harness..."
	cargo run --bin bench
