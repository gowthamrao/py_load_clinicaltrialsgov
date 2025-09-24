# Makefile for py-load-clinicaltrialsgov

# Default shell
SHELL := /bin/bash

# Activate the virtual environment
VENV_ACTIVATE := . .venv/bin/activate

# Default target
.PHONY: help
help:
	@echo "Commands:"
	@echo "  install          - Install dependencies"
	@echo "  lint             - Run linter"
	@echo "  test             - Run tests"
	@echo "  db-up            - Start postgres container"
	@echo "  db-down          - Stop postgres container"
	@echo "  init-db          - Initialize the database"
	@echo "  run-full         - Run a full ETL load"

# Install dependencies
.PHONY: install
install:
	uv venv
	$(VENV_ACTIVATE) && uv pip install -e .[dev]

# Run linter
.PHONY: lint
lint:
	$(VENV_ACTIVATE) && ruff check . && mypy src

# Run tests
.PHONY: test
test:
	$(VENV_ACTIVATE) && pytest

# Start postgres container
.PHONY: db-up
db-up:
	docker compose up -d

# Stop postgres container
.PHONY: db-down
db-down:
	docker compose down

# Initialize the database
.PHONY: init-db
init-db:
	$(VENV_ACTIVATE) && export DB_DSN="postgresql://user:password@localhost:5432/ctg" && py-load-clinicaltrialsgov migrate-db

# Run a full ETL load
.PHONY: run-full
run-full:
	$(VENV_ACTIVATE) && export DB_DSN="postgresql://user:password@localhost:5432/ctg" && py-load-clinicaltrialsgov run --load-type full
