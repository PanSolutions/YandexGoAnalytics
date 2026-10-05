from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.core.services.data_aggregation.base_aggregation import BaseAggregationService


class UserGoldMetricsService(BaseAggregationService):
    def __init__(self) -> None:
        super().__init__(source_table_name="users", target_table_name="user_metrics")

    def transform_stream(self, df: DataFrame) -> DataFrame:
        return (
            df.groupBy("registration_date")
            .agg(F.count("*").alias("users_count"))
            .withColumn("_calculated_at", F.current_timestamp())
        )
