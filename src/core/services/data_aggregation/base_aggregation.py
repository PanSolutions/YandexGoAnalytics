from __future__ import annotations

import os
from abc import ABC, abstractmethod

from dotenv import load_dotenv
from loguru import logger
from pyspark.sql import DataFrame, SparkSession

load_dotenv()


class BaseAggregationService(ABC):
    def __init__(
        self,
        source_table_name: str,
        target_table_name: str,
    ) -> None:
        self.source_table_name = source_table_name
        self.target_table_name = target_table_name

        self.catalog = os.environ.get("DATABRICKS_CATALOG")
        self.full_source_table = f"{self.catalog}.silver.{self.source_table_name}"
        self.full_target_table = f"{self.catalog}.gold.{self.target_table_name}"

        self.checkpoint_path = (
            f"/Volumes/{self.catalog}/raw_files/landing/_checkpoints/gold/{self.target_table_name}"
        )

    def extract_stream(self, spark: SparkSession) -> DataFrame:
        logger.info(f"Reading incremental stream from: {self.full_source_table}")
        return (
            spark.readStream.format("delta")
            .option("ignoreChanges", "true")
            .table(self.full_source_table)
        )

    @abstractmethod
    def transform_stream(self, df: DataFrame) -> DataFrame: ...

    def optimize(self, spark: SparkSession) -> None:
        logger.info(f"Running OPTIMIZE on {self.full_target_table}")
        spark.sql(f"OPTIMIZE {self.full_target_table}")

    def run(self, spark: SparkSession) -> None:
        logger.info(
            f"Starting Streaming Aggregation with Checkpoints: {self.full_source_table} -> {self.full_target_table}"
        )

        stream_df = self.extract_stream(spark)

        agg_stream = self.transform_stream(stream_df)

        query = (
            agg_stream.writeStream.format("delta")
            .outputMode("complete")
            .option("checkpointLocation", f"{self.checkpoint_path}/data")
            .trigger(availableNow=True)
            .toTable(self.full_target_table)
        )

        query.awaitTermination()

        self.optimize(spark)
        logger.info(f"Successfully updated {self.full_target_table} via checkpoints!")
