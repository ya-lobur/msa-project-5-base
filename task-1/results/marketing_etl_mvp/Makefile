.PHONY: help build up down logs clean lint format test

help:
	@echo "Available commands:"
	@echo "  make build   - Build Docker images"
	@echo "  make up      - Start all services"
	@echo "  make down    - Stop all services"
	@echo "  make logs    - View logs"
	@echo "  make clean   - Remove all containers and volumes"
	@echo "  make lint    - Run ruff linter"
	@echo "  make format  - Format code with ruff"
	@echo "  make test    - Run tests"

build:
	docker compose build

up:
	docker compose up -d
	@echo "Airflow UI: http://localhost:8080 (airflow/airflow)"
	@echo "MailHog UI: http://localhost:8025"

down:
	docker compose down

logs:
	docker compose logs -f

clean:
	docker compose down -v
	rm -rf logs/* data/*

lint:
	uv run ruff check src/ dags/

format:
	uv run ruff format src/ dags/

test:
	docker compose exec airflow-webserver pytest tests/
