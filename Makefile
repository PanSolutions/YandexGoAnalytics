-include .env
export

TARGET ?= dev

.PHONY: check-env validate deploy deploy-dev deploy-stage deploy-prod run destroy

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
