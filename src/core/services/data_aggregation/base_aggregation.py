from __future__ import annotations

from abc import ABC, abstractmethod
import os

from loguru import logger
from pyspark.sql import DataFrame, SparkSession


class BaseAggregationService(ABC):

    def __init__(
        self,
        source_table_name: str,
        target_table_name: str,
    ) -> None:
        self.source_table_name = source_table_name
        self.target_table_name = target_table_name

        self.catalog = os.environ.get("DATABRICKS_CATALOG", "yandex_go_dev")
        self.full_source_table = (
            f"{self.catalog}.silver.{self.source_table_name}"
        )
        self.full_target_table = (
            f"{self.catalog}.gold.{self.target_table_name}"
        )
        self.checkpoint_path = f"/Volumes/{self.catalog}/raw_files/landing/_checkpoints/gold/{self.target_table_name}"

    def extract(self, spark: SparkSession) -> DataFrame:
        logger.info(
            f"Reading incremental stream from: {self.full_source_table}"
        )
        return (
            spark.readStream.format("delta")
            .option("ignoreChanges", "true")
            .table(self.full_source_table)
        )

    @abstractmethod
    def upsert_micro_batch(
        self, micro_batch_df: DataFrame, batch_id: int
    ) -> None:
        ...

    def optimize(self, spark: SparkSession) -> None:
        logger.info(f"Running OPTIMIZE on {self.full_target_table}")
        spark.sql(f"OPTIMIZE {self.full_target_table}")

    def run(self, spark: SparkSession) -> None:
        logger.info(
            f"Starting incremental aggregation for: {self.full_target_table}"
        )

        stream_df = self.extract(spark)

        query = (
            stream_df.writeStream.format("delta")
            .foreachBatch(self.upsert_micro_batch)
            .option("checkpointLocation", f"{self.checkpoint_path}/data")
            .trigger(availableNow=True)
            .start()
        )

        query.awaitTermination()
        self.optimize(spark)
        logger.info(f"Successfully updated and optimized {self.full_target_table}")
