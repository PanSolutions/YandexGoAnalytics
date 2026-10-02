-include .env
export

TARGET ?= dev

.PHONY: check-env validate deploy deploy-dev deploy-stage deploy-prod run destroy \
        format lint-fix fix lint format-check typecheck check check-all


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


check-all: check validate
	@echo "Code and Databricks Bundle are 100% ready for deployment!"

check-env:
ifndef DATABRICKS_HOST
	$(error DATABRICKS_HOST is not set. Check your .env file)
endif

validate: check-env
	@echo "Validating Databricks bundle for target: [$(TARGET)]"
	databricks bundle validate -t $(TARGET)
	@echo "Bundle configuration is valid."

deploy: check-env
	@echo "Deploying bundle to target: [$(TARGET)]"
	databricks bundle deploy -t $(TARGET)
	@echo "Deployment to [$(TARGET)] completed successfully."

deploy-dev: check-env
	@echo "Deploying bundle to DEV environment..."
	databricks bundle deploy -t dev
	@echo "DEV deployment completed."

deploy-stage: check-env
	@echo "Deploying bundle to STAGING environment..."
	databricks bundle deploy -t staging
	@echo "STAGING deployment completed."

deploy-prod: check-env
	@echo "Deploying bundle to PROD environment..."
	databricks bundle deploy -t prod
	@echo "PROD deployment completed."

test:
	@echo "Running Unit tests..."
	python -m pytest src/tests/unit -v

test-integration: check-env
	@echo "Running Integration tests..."
	python -m pytest src/tests/integration -v


security:
	@echo "Running Bandit SAST..."
	python -m bandit -r src/ -ll
	@echo "Checking dependencies for CVEs..."
	python -m pip_audit

docs:
	mkdocs serve

docs-build:
	mkdocs build --strict
