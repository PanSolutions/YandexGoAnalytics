from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .base import BaseAggregationService


class UserGoldMetricsService(BaseAggregationService):
    """Build ``gold.user_metrics``: number of registrations per day."""

    def __init__(self) -> None:
        """Bind the service to ``silver.users`` and ``gold.user_metrics``."""
        super().__init__(source_table_name="users", target_table_name="user_metrics")

    def transform_stream(self, df: DataFrame) -> DataFrame:
        """Count users by ``registration_date``."""
        return (
            df.groupBy("registration_date")
            .agg(F.count("*").alias("users_count"))
            .withColumn("_calculated_at", F.current_timestamp())
        )
