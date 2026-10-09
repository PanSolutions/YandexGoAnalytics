-include .env
export

TARGET        ?= dev
VALID_TARGETS := dev staging prod
TRIPS_PIPELINE := yandex_go_trips_pipeline

.DEFAULT_GOAL := help

# Help
.PHONY: help
help: ## Show this help
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  %-24s %s\n", $$1, $$2}'

# Environment guards
.PHONY: check-env check-target

check-env:
ifndef DATABRICKS_HOST
	$(error DATABRICKS_HOST is not set. Check your .env file)
endif

check-target:
ifeq ($(filter $(TARGET),$(VALID_TARGETS)),)
	$(error Unknown TARGET '$(TARGET)'. Use one of: $(VALID_TARGETS))
endif


# Code quality
.PHONY: format lint-fix fix format-check lint typecheck check

format:
	@echo "Formatting code with Ruff..."
	ruff format .

lint-fix:
	@echo "Auto-fixing linter errors with Ruff..."
	ruff check --fix .

fix: format lint-fix
	@echo "Code formatted and auto-fixed!"

format-check:
	@echo "Checking code formatting..."
	ruff format --check .

lint:
	@echo "Running Ruff linter..."
	ruff check .

typecheck:
	@echo "Running MyPy typechecker..."
	mypy src/

check: format-check lint typecheck
	@echo "All code checks passed successfully!"

# Tests and security
.PHONY: test test-integration security

test:
	@echo "Running unit tests..."
	python -m pytest tests/unit -v

test-integration: check-env
	@echo "Running integration tests..."
	python -m pytest tests/integration -v

security:
	@echo "Running Bandit SAST..."
	python -m bandit -r src/ -ll
	@echo "Checking dependencies for CVEs..."
	python -m pip_audit

# Databricks bundle
.PHONY: validate deploy deploy-dev deploy-stage deploy-prod check-all

validate: check-env check-target
	@echo "Validating Databricks bundle for target: [$(TARGET)]"
	databricks bundle validate -t $(TARGET)
	@echo "Bundle configuration is valid."

deploy: check-env check-target
	@echo "Deploying bundle to target: [$(TARGET)]"
	databricks bundle deploy -t $(TARGET)
	@echo "Deployment to [$(TARGET)] completed successfully."

deploy-dev:
	@$(MAKE) --no-print-directory deploy TARGET=dev

deploy-stage:
	@$(MAKE) --no-print-directory deploy TARGET=staging

deploy-prod:
	@$(MAKE) --no-print-directory deploy TARGET=prod

check-all: check validate
	@echo "Code and Databricks Bundle are 100% ready for deployment!"

# Pipelines
.PHONY: run-trips run-trips-dev deploy-and-run-trips

run-trips: check-env check-target
	@echo "Running Trips DLT pipeline on target: [$(TARGET)]..."
	databricks bundle run $(TRIPS_PIPELINE) -t $(TARGET)

run-trips-dev:
	@$(MAKE) --no-print-directory run-trips TARGET=dev

deploy-and-run-trips: deploy run-trips

# Docs
.PHONY: docs docs-build

docs:
	python -m mkdocs serve

docs-build:
	python -m mkdocs build --strict
