# Yandex Go Analytics

**A Data Lakehouse platform on Databricks for ride-hailing analytics.**
Raw trip, user and driver data lands in a Unity Catalog volume and flows through a
**Medallion architecture** (Bronze → Silver → Gold) into business-ready metrics.
The whole platform is deployed as code with Databricks Asset Bundles and guarded by CI.

<div class="grid cards" markdown>

- :material-database-arrow-right: **Medallion pipelines**

    Incremental Auto Loader ingestion, validated Silver tables and Gold metrics,
    implemented twice: as reusable Python services and as a Delta Live Tables pipeline.

- :material-rocket-launch: **Deploy as code**

    Schemas, volumes, pipeline and scheduled job are defined in `resources/` and shipped to
    `dev`, `staging` and `prod` with `databricks bundle deploy`.

- :material-shield-check: **Quality gates**

    Ruff, MyPy, unit and integration tests, Bandit, pip-audit and Gitleaks run in CI.

- :material-book-open-variant: **Living documentation**

    This site is built from the code: the [API Reference](api.md) is generated from docstrings.

</div>

## What the platform does

| Entity | Landing format | Ingestion style | Final analytics |
|--------|----------------|-----------------|-----------------|
| **Trips** (taxi rides) | Parquet | Batch (Auto Loader) | Revenue and trip counts by pickup zone and payment type; trips enriched with driver details |
| **Users** (passengers) | Avro | Batch (Auto Loader) | Registrations per day |
| **Drivers** | JSON files (stream) | Streaming (Auto Loader) | Driver count and average rating by experience |

Source data is synthetic and produced by the [data generators](api.md#data-generation), which
deliberately inject invalid rows so that every Silver quality rule has something to catch.

## Tech stack

| Area | Technology |
|------|------------|
| Platform | Databricks serverless, Unity Catalog, Delta Lake, Auto Loader, Delta Live Tables |
| Language | Python 3.12, PySpark, `databricks-connect` |
| Data models | Spark `StructType` schemas, Pydantic v2 (audit report) |
| Test data | Faker |
| Logging | Loguru |
| Deployment | Databricks Asset Bundles (`databricks.yml`, `resources/`) |
| Quality | Ruff, MyPy, pytest, pre-commit |
| Security | Bandit, pip-audit, Gitleaks |
| Docs | MkDocs Material, mkdocstrings |

## Environments

| Target | Catalog | Git branch | Bundle mode |
|--------|---------|------------|-------------|
| `dev` (default) | `yandex_go_dev` | `development` | development |
| `staging` | `yandex_go_staging` | `staging` | production |
| `prod` | `yandex_go_prod` | `production` | production |

A push to a branch deploys the matching target automatically; see
[CI/CD](architecture.md#cicd).

## Quick start

!!! info "Prerequisites"
    Python 3.12, [uv](https://docs.astral.sh/uv/) (or pip), the
    [Databricks CLI](https://docs.databricks.com/dev-tools/cli/) and access to a Databricks workspace.

**1. Install and configure**

```bash
git clone https://github.com/stanislavpanfilenko/YandexGoAnalytics.git
cd YandexGoAnalytics

uv sync                 # or: pip install -e .
cp .env.example .env    # then fill in the values below
```

```dotenv
DATABRICKS_HOST=https://<your-workspace>.cloud.databricks.com
DATABRICKS_CATALOG=yandex_go_dev
DATABRICKS_ENV=dev
```

Authenticate with `databricks auth login` or by exporting `DATABRICKS_TOKEN`.

**2. Check the code**

```bash
make check      # ruff format check + lint + mypy
make test       # unit tests (no Databricks needed)
```

**3. Deploy and run the pipeline**

```bash
make validate               # databricks bundle validate
make deploy-dev             # create schemas, volumes, pipeline and job in dev
make run-trips-dev          # run the Trips DLT pipeline
```

**4. Generate data and use the services from Python**

```python
from src.core.infra.connection.databricks_connector import DatabricksConnectionService
from src.core.utils.data_generation import DriverGenerator, TripGenerator, UserGenerator
from src.core.etl.bronze import TaxiTripBatchIngestionService

with DatabricksConnectionService("yandex-go") as conn:
    spark = conn.spark

    # 1. Put synthetic files into the landing volume
    TripGenerator(subfolder="parquet").generate(spark, row_count=1000)
    UserGenerator(subfolder="avro").generate(spark, row_count=500)
    DriverGenerator(subfolder="json").generate(spark, row_count=100)

    # 2. Ingest them into Bronze (Silver and Gold services are used the same way)
    TaxiTripBatchIngestionService().run(spark)
```

!!! tip "Match the folders"
    Generators and ingestion services each have their own default folder names. Pass the same
    `subfolder` to both sides (as above), otherwise the ingestion finds no files.

**5. Build these docs**

```bash
make docs          # live preview on http://127.0.0.1:8000
make docs-build    # strict build, the same check CI runs
```

## Make targets

Run `make` without arguments to print the list. The most useful ones:

| Target | Purpose |
|--------|---------|
| `make fix` | Format and auto-fix lint errors |
| `make check` | Format check, lint and type check |
| `make test` / `make test-integration` | Unit tests / integration tests against Databricks |
| `make security` | Bandit and pip-audit |
| `make validate` | Validate the bundle for `TARGET` (dev, staging or prod) |
| `make deploy-dev` / `deploy-stage` / `deploy-prod` | Deploy to an environment |
| `make run-trips` | Run the Trips pipeline on `TARGET` |
| `make deploy-and-run-trips` | Deploy, then run |
| `make docs` / `make docs-build` | Serve / build this documentation |

## Repository layout

```text
.
├── databricks.yml               # bundle: variables and targets (dev, staging, prod)
├── resources/                   # bundle resources
│   ├── schemas.yml              #   raw_files, bronze, silver, gold
│   ├── volumes.yml              #   landing volume
│   ├── pipelines/               #   Trips DLT pipeline
│   ├── jobs/                    #   daily scheduled run
│   └── dashboards/              #   Lakeview dashboards (currently disabled)
├── src/
│   ├── core/                    # reusable Python package
│   │   ├── config/              #   environment variables
│   │   ├── infra/               #   Databricks connection, audit report
│   │   ├── schemas/             #   Spark schemas and Pydantic report models
│   │   ├── etl/                 #   bronze / silver / gold services
│   │   └── utils/data_generation/   synthetic data generators
│   ├── dlt_pipelines/           # Delta Live Tables notebooks (bronze, silver, gold)
│   └── notebooks/               # step-by-step walkthrough notebooks
├── tests/                       # unit/, integration/, fixtures/
├── docs/                        # this site
└── .github/workflows/           # CI/CD
```

## Where to go next

- [Architecture](architecture.md): data flow, layers, design decisions, deployment and CI/CD.
- [API Reference](api.md): every module, class and function, generated from the docstrings.
