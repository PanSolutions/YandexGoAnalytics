from __future__ import annotations

from loguru import logger
from pyspark.sql import SparkSession

from src.core.config import get_catalog, get_environment
from src.core.schemas import (
    ReportSummary,
    TableAuditMetadata,
    TableStatus,
    WorkflowAuditReport,
)


class WorkflowReportService:
    """Build and persist an audit report of the Medallion tables.

    The report contains existence, row count and latest Delta history entry for
    every monitored table and is saved as JSON into a Unity Catalog volume.
    """

    def __init__(self) -> None:
        """Resolve the catalog and the list of monitored tables."""
        self.catalog = get_catalog()
        self.output_path = (
            f"/Volumes/{self.catalog}/raw_files/landing/reports/workflow_summary.json"
        )

        self.tables: dict[str, list[str]] = {
            "bronze": ["taxi", "drivers", "users"],
            "silver": ["taxi", "drivers", "users"],
            "gold": [
                "taxi_metrics",
                "driver_metrics",
                "user_metrics",
                "enriched_trips",
            ],
        }

    def _get_table_metadata(
        self, spark: SparkSession, schema: str, table_name: str
    ) -> TableAuditMetadata:
        """Collect audit metadata for a single table.

        Args:
            spark: Active Spark session.
            schema: Schema (layer) name, e.g. ``bronze``.
            table_name: Table name without catalog and schema.

        Returns:
            Metadata with status ``HEALTHY``, ``NOT_FOUND`` or ``ERROR``.
            Errors are captured in the result and are not raised.
        """
        full_name = f"{self.catalog}.{schema}.{table_name}"
        try:
            if not spark.catalog.tableExists(full_name):
                return TableAuditMetadata(
                    exists=False,
                    row_count=0,
                    status=TableStatus.NOT_FOUND,
                )

            row_count = spark.table(full_name).count()
            history_rows = spark.sql(f"DESCRIBE HISTORY {full_name} LIMIT 1").collect()

            latest_version = history_rows[0]["version"] if history_rows else None
            latest_operation = history_rows[0]["operation"] if history_rows else None
            last_modified = str(history_rows[0]["timestamp"]) if history_rows else None

            return TableAuditMetadata(
                exists=True,
                row_count=row_count,
                latest_version=latest_version,
                latest_operation=latest_operation,
                last_modified=last_modified,
                status=TableStatus.HEALTHY,
            )

        except Exception as e:
            return TableAuditMetadata(
                exists=False,
                row_count=0,
                status=TableStatus.ERROR,
                error=str(e),
            )

    def generate_report(self, spark: SparkSession) -> WorkflowAuditReport:
        """Audit all monitored tables and try to save the report to the volume.

        Args:
            spark: Active Spark session.

        Returns:
            The generated report. A failure to upload it is logged as a warning
            and does not affect the returned value.
        """
        logger.info(f"Generating Pydantic-validated Audit Report for [{self.catalog}]...")

        layers_report: dict[str, dict[str, TableAuditMetadata]] = {}
        total_records = 0

        for schema, table_list in self.tables.items():
            layers_report[schema] = {}
            for table in table_list:
                meta = self._get_table_metadata(spark, schema, table)
                layers_report[schema][table] = meta
                total_records += meta.row_count

        report = WorkflowAuditReport(
            pipeline_name="yandex-go-analytics",
            environment=get_environment(),
            catalog=self.catalog,
            summary=ReportSummary(
                total_monitored_tables=sum(len(t) for t in self.tables.values()),
                total_rows_across_lakehouse=total_records,
                pipeline_status="SUCCESS",
            ),
            layers=layers_report,
        )

        try:
            import io

            from databricks.sdk import WorkspaceClient

            w = WorkspaceClient()
            report_bytes = io.BytesIO(report.model_dump_json(indent=2).encode("utf-8"))

            w.files.upload(
                file_path=self.output_path,
                contents=report_bytes,
                overwrite=True,
            )
            logger.info(f"Audit report successfully saved to: {self.output_path}")

        except Exception as e:
            logger.warning(f"Could not persist report to Volume: {e}")

        return report
