from __future__ import annotations

import os

from dotenv import load_dotenv
from loguru import logger
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery

load_dotenv()


class BaseIngestionService:
    def __init__(
        self,
        source_subfolder: str,
        target_table_name: str,
        source_format: str,
        volume_name: str = "landing",
        target_schema: str = "bronze",
    ) -> None:
        self.source_format = source_format
        self.source_subfolder = source_subfolder.strip("/")
        self.volume_name = volume_name
        self.target_schema = target_schema
        self.target_table_name = target_table_name

        self.catalog = os.environ.get("DATABRICKS_CATALOG")

        self.source_path = (
            f"/Volumes/{self.catalog}/raw_files/{self.volume_name}/{self.source_subfolder}"
        )
        self.checkpoint_path = (
            f"/Volumes/{self.catalog}/raw_files/{self.volume_name}/_checkpoints/"
            f"{self.target_schema}/{self.target_table_name}"
        )
        self.full_target_table = f"{self.catalog}.{self.target_schema}.{self.target_table_name}"

    def extract(self, spark: SparkSession) -> DataFrame:
        logger.info(f"Reading data [{self.source_format}] from: {self.source_path}")
        return (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", self.source_format)
            .option("cloudFiles.schemaLocation", f"{self.checkpoint_path}/schema")
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
            .option("cloudFiles.includeExistingFiles", "true")
            .load(self.source_path)
        )

    @staticmethod
    def add_audit_metadata(df: DataFrame) -> DataFrame:
        return df.withColumn("_ingested_at", F.current_timestamp()).withColumn(
            "_source_file", F.col("_metadata.file_path")
        )

    def load(self, df: DataFrame) -> None | StreamingQuery:
        logger.info(f"Writing data in table Delta: {self.full_target_table}")

        query = (
            df.writeStream.format("delta")
            .option("checkpointLocation", f"{self.checkpoint_path}/data")
            .option("mergeSchema", "true")
            .trigger(availableNow=True)
            .toTable(self.full_target_table)
        )

        query.awaitTermination()

        logger.info(f"Successfully loaded in {self.full_target_table}")
        return None

    def run(self, spark: SparkSession) -> StreamingQuery | None:
        logger.info(f"Start data ingestion for {self.full_target_table}")
        raw_df = self.extract(spark)
        df_with_metadata = self.add_audit_metadata(raw_df)
        self.load(df_with_metadata)
        return None
