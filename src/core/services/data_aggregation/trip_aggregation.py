from __future__ import annotations

from delta.tables import DeltaTable
from loguru import logger
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.core.services.data_aggregation.base_aggregation import BaseAggregationService


class TripGoldMetricsService(BaseAggregationService):
    """
    - group by pu_location_id, payment_type_description
    - total_trips, total_revenue (rounded to 2 decimal places)
    - Liquid Clustering по PULocationID
    """

    def upsert_micro_batch(self, micro_batch_df: DataFrame, batch_id: int) -> None:
        if micro_batch_df.isEmpty():
            return

        spark = micro_batch_df.sparkSession

        batch_agg = micro_batch_df.groupBy("pu_location_id", "payment_type_description").agg(
            F.count("*").alias("batch_trips"),
            F.round(F.sum("fare_amount"), 2).alias("batch_revenue"),
        )

        if not spark.catalog.tableExists(self.full_target_table):
            logger.info(
                f"Creating initial table {self.full_target_table} with Liquid Clustering..."
            )
            (
                batch_agg.select(
                    "pu_location_id",
                    "payment_type_description",
                    F.col("batch_trips").alias("total_trips"),
                    F.col("batch_revenue").alias("total_revenue"),
                    F.current_timestamp().alias("_calculated_at"),
                )
                .write.format("delta")
                .clusterBy("pu_location_id")
                .saveAsTable(self.full_target_table)
            )
            return

        gold_table = DeltaTable.forName(spark, self.full_target_table)

        (
            gold_table.alias("target")
            .merge(
                batch_agg.alias("source"),
                """
                target.pu_location_id = source.pu_location_id AND
                target.payment_type_description = source.payment_type_description
                """,
            )
            .whenMatchedUpdate(
                set={
                    "total_trips": "target.total_trips + source.batch_trips",
                    "total_revenue": "round(target.total_revenue + source.batch_revenue, 2)",
                    "_calculated_at": "current_timestamp()",
                }
            )
            .whenNotMatchedInsert(
                values={
                    "pu_location_id": "source.pu_location_id",
                    "payment_type_description": "source.payment_type_description",
                    "total_trips": "source.batch_trips",
                    "total_revenue": "source.batch_revenue",
                    "_calculated_at": "current_timestamp()",
                }
            )
            .execute()
        )
