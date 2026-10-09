# API Reference

This page is **generated from the docstrings** in `src/core` by
[mkdocstrings](https://mkdocstrings.github.io/): change a docstring, rebuild the docs, and this
page updates. Each entry shows the signature, a description, parameters, return values, raised
exceptions and (collapsed) the source code.

!!! tip "How to read this page"
    Modules are grouped by role, in the order data travels:
    [Configuration](#configuration) → [Schemas](#schemas) →
    [Infrastructure](#infrastructure) → [Data generation](#data-generation) →
    [Bronze](#bronze-ingestion) → [Silver](#silver-transformation) → [Gold](#gold-aggregation).
    Base classes come first in each layer; entity classes below them only override what differs.
    For the big picture see [Architecture](architecture.md).

## Configuration

Single entry point for environment variables.

::: src.core.config.settings

## Schemas

Spark schemas for each entity and the Pydantic models of the audit report.

**Entity schemas**

::: src.core.schemas.base

::: src.core.schemas.driver

::: src.core.schemas.user

::: src.core.schemas.trip

**Audit report models**

::: src.core.schemas.reporting

## Infrastructure

**Databricks connection**

::: src.core.infra.connection.base_connector

::: src.core.infra.connection.databricks_connector

**Audit report**

::: src.core.infra.audit.reporting

## Data generation

Synthetic data generators that write files to the landing volume.

::: src.core.utils.data_generation.base

::: src.core.utils.data_generation.driver

::: src.core.utils.data_generation.user

::: src.core.utils.data_generation.trip

## Bronze ingestion

Auto Loader ingestion from the landing volume into raw Delta tables.

::: src.core.etl.bronze.base

::: src.core.etl.bronze.driver

::: src.core.etl.bronze.user

::: src.core.etl.bronze.trip

## Silver transformation

Cleaning, validation and deduplication.

::: src.core.etl.silver.base

::: src.core.etl.silver.driver

::: src.core.etl.silver.user

::: src.core.etl.silver.trip

## Gold aggregation

Business metrics and enriched datasets.

::: src.core.etl.gold.base

::: src.core.etl.gold.driver

::: src.core.etl.gold.user

::: src.core.etl.gold.trip

::: src.core.etl.gold.trip_driver
