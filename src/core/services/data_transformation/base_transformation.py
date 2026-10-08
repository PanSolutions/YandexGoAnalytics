from __future__ import annotations

import os
from abc import ABC, abstractmethod

from dotenv import load_dotenv
from loguru import logger
from pyspark.sql import DataFrame, SparkSession

load_dotenv()


class BaseTransformationService(ABC):
    def __init__(
        self,
        table_name: str,
        source_schema: str = "bronze",
        target_schema: str = "silver",
    ) -> None:
        self.table_name = table_name
        self.source_schema = source_schema
        self.target_schema = target_schema

        self.catalog = os.environ.get("DATABRICKS_CATALOG")
        self.full_source_table = f"{self.catalog}.{self.source_schema}.{self.table_name}"
        self.full_target_table = f"{self.catalog}.{self.target_schema}.{self.table_name}"

        self.checkpoint_path = (
            f"/Volumes/{self.catalog}/raw_files/landing/_checkpoints/"
            f"{self.target_schema}/{self.table_name}"
        )

    def extract(self, spark: SparkSession) -> DataFrame:
        logger.info(f"Reading incremental stream from Bronze: {self.full_source_table}")
        return (
            spark.readStream.format("delta")
            .option("ignoreChanges", "true")
            .table(self.full_source_table)
        )

    @abstractmethod
    def transform(self, df: DataFrame) -> DataFrame: ...

    def load(self, df: DataFrame) -> None:
        logger.info(f"Writing incremental batch to Silver: {self.full_target_table}")

        query = (
            df.writeStream.format("delta")
            .outputMode("append")
            .option("checkpointLocation", f"{self.checkpoint_path}/data")
            .option("mergeSchema", "true")
            .trigger(availableNow=True)
            .toTable(self.full_target_table)
        )

        query.awaitTermination()
        logger.info(f"Successfully processed new rows into {self.full_target_table}")

    def run(self, spark: SparkSession) -> None:
        logger.info(
            f"Starting incremental transformation: {self.full_source_table} -> {self.full_target_table}"
        )
        new_bronze_rows = self.extract(spark)
        cleansed_df = self.transform(new_bronze_rows)
        self.load(cleansed_df)
