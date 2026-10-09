from __future__ import annotations

from unittest.mock import MagicMock, patch

from pyspark.sql import SparkSession

from src.core.infra.audit.reporting import WorkflowReportService
from src.core.schemas.reporting import TableStatus


class TestWorkflowReportService:
    """Test suite verifying lakehouse audit report collection and pydantic models."""

    @patch("src.core.infra.audit.reporting.get_catalog", return_value="test_cat")
    @patch("src.core.infra.audit.reporting.get_environment", return_value="dev")
    def test_table_metadata_not_found(
        self, _: object, __: object, spark_session: SparkSession
    ) -> None:
        """Ensure missing table is handled gracefully with NOT_FOUND status."""
        reporter = WorkflowReportService()

        # Эмулируем отсутствие таблицы
        spark_session.catalog.tableExists.return_value = False

        metadata = reporter._get_table_metadata(spark_session, "bronze", "missing_table")
        assert not metadata.exists
        assert metadata.status == TableStatus.NOT_FOUND
        assert metadata.row_count == 0

    @patch("src.core.infra.audit.reporting.get_catalog", return_value="test_cat")
    @patch("src.core.infra.audit.reporting.get_environment", return_value="dev")
    def test_table_metadata_error_handling(
        self, _: object, __: object, spark_session: SparkSession
    ) -> None:
        """Ensure unexpected Spark catalog error produces TableStatus.ERROR."""
        reporter = WorkflowReportService()

        # Эмулируем сбой запроса к каталогу
        with patch.object(
            spark_session.catalog, "tableExists", side_effect=Exception("Catalog unreachable")
        ):
            metadata = reporter._get_table_metadata(spark_session, "bronze", "taxi")
            assert not metadata.exists
            assert metadata.status == TableStatus.ERROR
            assert "Catalog unreachable" in str(metadata.error)

    @patch("src.core.infra.audit.reporting.get_catalog", return_value="test_cat")
    @patch("src.core.infra.audit.reporting.get_environment", return_value="dev")
    @patch("databricks.sdk.WorkspaceClient")
    def test_generate_report_summary(
        self,
        mock_ws_client: MagicMock,
        _: object,
        __: object,
        spark_session: SparkSession,
    ) -> None:
        """Validate end-to-end report model generation and Databricks upload triggering."""
        reporter = WorkflowReportService()
        spark_session.catalog.tableExists.return_value = False

        report = reporter.generate_report(spark_session)

        assert report.pipeline_name == "yandex-go-analytics"
        assert report.environment == "dev"
        assert report.catalog == "test_cat"
        assert report.summary.total_monitored_tables == 10
        assert report.summary.pipeline_status == "SUCCESS"
        mock_ws_client.return_value.files.upload.assert_called_once()
