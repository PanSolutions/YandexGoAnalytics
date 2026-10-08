from __future__ import annotations

from datetime import UTC, datetime
from enum import Enum
from typing import Literal

from pydantic import BaseModel, Field


class TableStatus(Enum):
    HEALTHY = "HEALTHY"
    NOT_FOUND = "NOT_FOUND"
    ERROR = "ERROR"


class TableAuditMetadata(BaseModel):
    """Metadata for an individual Lakehouse Delta table."""

    exists: bool
    row_count: int = Field(ge=0, description="Total row count (>= 0)")
    latest_version: int | None = Field(
        default=None, description="Latest Delta Lake transaction version"
    )
    latest_operation: str | None = Field(
        default=None,
        description="Latest Delta transaction operation (e.g., STREAMING UPDATE, MERGE, WRITE)",
    )
    last_modified: str | None = Field(
        default=None, description="Timestamp of the last modification"
    )
    status: TableStatus
    error: str | None = None


class LayerAuditReport(BaseModel):
    """Audit report for a specific Medallion Architecture layer (Bronze, Silver, Gold)."""

    tables: dict[str, TableAuditMetadata]


class ReportSummary(BaseModel):
    """High-level summary metrics across the Lakehouse platform."""

    total_monitored_tables: int = Field(gt=0, description="Total number of monitored tables")
    total_rows_across_lakehouse: int = Field(
        ge=0, description="Total record count across all Medallion layers"
    )
    pipeline_status: Literal["SUCCESS", "FAILED", "PARTIAL"]


class WorkflowAuditReport(BaseModel):
    """Root Pydantic data contract for the workflow audit report."""

    pipeline_name: str
    environment: str
    catalog: str
    generated_at: datetime = Field(
        default_factory=lambda: datetime.now(UTC),
        description="UTC timestamp of report generation",
    )
    summary: ReportSummary
    layers: dict[str, dict[str, TableAuditMetadata]]
