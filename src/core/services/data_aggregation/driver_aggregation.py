from __future__ import annotations

from delta.tables import DeltaTable
from loguru import logger
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.core.services.data_aggregation.base_aggregation import BaseAggregationService


class DriverGoldMetricsService(BaseAggregationService):
    """
    - group by experience
    - drivers_count, avg_rating (rounded to 2 decimal places)
    - Z-Order optimization by experience
    """

    def __init__(self) -> None:
        super().__init__(source_table_name="drivers", target_table_name="driver_metrics")

    def optimize(self, spark: SparkSession) -> None:
        logger.info(f"Running Z-Order on {self.full_target_table}")
        spark.sql(f"OPTIMIZE {self.full_target_table} ZORDER BY (experience)")

    def upsert_micro_batch(self, micro_batch_df: DataFrame, batch_id: int) -> None:
        if micro_batch_df.isEmpty():
            return

        spark = micro_batch_df.sparkSession

        batch_agg = micro_batch_df.groupBy("experience").agg(
            F.count("*").alias("batch_drivers"),
            F.sum("rating").alias("batch_rating_sum"),
        )

        if not spark.catalog.tableExists(self.full_target_table):
            (
                batch_agg.select(
                    "experience",
                    F.col("batch_drivers").alias("drivers_count"),
                    F.col("batch_rating_sum").alias("total_rating_sum"),
                    F.round(F.col("batch_rating_sum") / F.col("batch_drivers"), 2).alias(
                        "avg_rating"
                    ),
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
                "target.experience = source.experience",
            )
            .whenMatchedUpdate(
                set={
                    "drivers_count": "target.drivers_count + source.batch_drivers",
                    "total_rating_sum": "target.total_rating_sum + source.batch_rating_sum",
                    "avg_rating": "round((target.total_rating_sum + source.batch_rating_sum) / (target.drivers_count + source.batch_drivers), 2)",
                    "_calculated_at": "current_timestamp()",
                }
            )
            .whenNotMatchedInsert(
                values={
                    "experience": "source.experience",
                    "drivers_count": "source.batch_drivers",
                    "total_rating_sum": "source.batch_rating_sum",
                    "avg_rating": "round(source.batch_rating_sum / source.batch_drivers, 2)",
                    "_calculated_at": "current_timestamp()",
                }
            )
            .execute()
        )
