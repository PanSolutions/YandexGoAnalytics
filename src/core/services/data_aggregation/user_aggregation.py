from __future__ import annotations

from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.core.services.data_aggregation.base_aggregation import BaseAggregationService


class UserGoldMetricsService(BaseAggregationService):
    """
    - count users per favorite_color
    - sorted descending by count
    """

    def __init__(self) -> None:
        super().__init__(source_table_name="users", target_table_name="user_metrics")

    def upsert_micro_batch(self, micro_batch_df: DataFrame, batch_id: int) -> None:
        if micro_batch_df.isEmpty():
            return

        spark = micro_batch_df.sparkSession

        batch_agg = micro_batch_df.groupBy("registration_date").agg(
            F.count("*").alias("batch_users")
        )

        if not spark.catalog.tableExists(self.full_target_table):
            (
                batch_agg.select(
                    "registration_date",
                    F.col("batch_users").alias("users_count"),
                    F.current_timestamp().alias("_calculated_at"),
                )
                .write.format("delta")
                .saveAsTable(self.full_target_table)
            )
            return

        gold_table = DeltaTable.forName(spark, self.full_target_table)

        (
            gold_table.alias("target")
            .merge(
                batch_agg.alias("source"),
                "target.registration_date = source.registration_date",
            )
            .whenMatchedUpdate(
                set={
                    "users_count": "target.users_count + source.batch_users",
                    "_calculated_at": "current_timestamp()",
                }
            )
            .whenNotMatchedInsert(
                values={
                    "registration_date": "source.registration_date",
                    "users_count": "source.batch_users",
                    "_calculated_at": "current_timestamp()",
                }
            )
            .execute()
        )
