.PHONY: help up down reset status setup seed capture worker api backfill reconcile test bench verify

help:
	@echo "Multi-Source Analytics Aggregation Platform - Available Targets:"
	@echo "  make up        - Start heterogeneous database containers (Analytics PG, Source PG 1 & 2, MySQL, MongoDB)"
	@echo "  make down      - Stop database containers"
	@echo "  make reset     - Stop containers, delete database volumes, and recreate from scratch"
	@echo "  make status    - Check health status of database containers"
	@echo "  make setup     - Run migrations on analytics DB and setup source triggers/indexes"
	@echo "  make seed      - Run synthetic data generator across heterogeneous sources"
	@echo "  make capture   - Run change capture daemon for all sources"
	@echo "  make worker    - Run aggregation worker daemon"
	@echo "  make api       - Run Axum REST API daemon"
	@echo "  make backfill  - Run worker backfill on existing source data"
	@echo "  make reconcile - Run oracle against current state and print diff report"
	@echo "  make test      - Run full cargo test suite across all workspace crates"
	@echo "  make bench     - Run benchmark suite"
	@echo "  make verify    - Automated full CI verification pipeline: up -> setup -> seed -> reconcile -> tests -> success"

up:
	@echo "Starting heterogeneous database containers..."
	docker compose up -d
	@echo "Waiting for all database instances to become healthy..."
	@docker compose ps
	@until docker compose exec -T analytics_db pg_isready -U postgres -d analytics_db > /dev/null 2>&1; do \
		echo "Waiting for Analytics PG (5434)..."; sleep 1; \
	done
	@until docker compose exec -T source_pg_1 pg_isready -U postgres -d source_db_1 > /dev/null 2>&1; do \
		echo "Waiting for Source PG 1 (5433)..."; sleep 1; \
	done
	@until docker compose exec -T source_pg_2 pg_isready -U postgres -d source_db_2 > /dev/null 2>&1; do \
		echo "Waiting for Source PG 2 (5435)..."; sleep 1; \
	done
	@until docker compose exec -T source_mysql mysqladmin ping -h localhost -u root -proot > /dev/null 2>&1; do \
		echo "Waiting for Source MySQL (3306)..."; sleep 1; \
	done
	@until docker compose exec -T source_mongo mongosh --port 27017 --eval "rs.status().ok" > /dev/null 2>&1; do \
		echo "Waiting for Source MongoDB rs0 (27017)..."; sleep 1; \
	done
	@echo "✅ All database instances (PG 5434, PG 5433, PG 5435, MySQL 3306, MongoDB 27017) are healthy and ready!"

down:
	@echo "Stopping database instances..."
	docker compose down

reset:
	@echo "Resetting database instances and wiping all volumes..."
	docker compose down -v
	$(MAKE) up

status:
	@docker compose ps

setup:
	@echo "Running database setup and migrations..."
	cargo run -p cli -- setup

seed:
	@echo "Running seeder..."
	cargo run -p cli -- seed

capture:
	@echo "Running capture daemon..."
	cargo run -p cli -- capture

worker:
	@echo "Running aggregation worker..."
	cargo run -p cli -- worker

api:
	@echo "Running Axum API..."
	cargo run -p cli -- api

backfill:
	@echo "Running backfill..."
	cargo run -p cli -- backfill

reconcile:
	@echo "Running oracle reconciliation..."
	cargo run -p cli -- reconcile

test:
	@echo "Running full test suite..."
	cargo test --workspace

bench:
	@echo "Running benchmark..."
	cargo run -p cli -- bench

verify:
	@echo "Starting full verification pipeline..."
	$(MAKE) up
	$(MAKE) setup
	@echo "Running multi-source seeding (1000 rows)..."
	cargo run -p cli -- seed --rows 1000 --tenants 20
	@echo "Running change capture pass..."
	cargo run -p cli -- capture
	@echo "Running worker processing pass..."
	cargo run -p cli -- worker
	@echo "Running reconciliation..."
	cargo run -p cli -- reconcile
	@echo "Running full test suite across workspace..."
	$(MAKE) test
	@echo "✅ Full verification passed cleanly!"
