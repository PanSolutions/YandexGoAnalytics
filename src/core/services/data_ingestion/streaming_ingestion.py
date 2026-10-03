from __future__ import annotations

from loguru import logger
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.streaming import StreamingQuery

from src.core.schemas.driver import DriverSchema
from src.core.services.data_ingestion import BaseIngestionService


class DriverStreamingIngestionService(BaseIngestionService):
    def __init__(
        self,
        source_subfolder: str = "drivers",
        target_table_name: str = "drivers",
        source_format: str = "json",
        trigger_interval: str = "2 seconds",
        include_phone_in_base_schema: bool = False,
    ) -> None:
        super().__init__(
            source_subfolder=source_subfolder,
            target_table_name=target_table_name,
            source_format=source_format,
        )
        self.trigger_interval = trigger_interval
        self.include_phone_in_base_schema = include_phone_in_base_schema

    def extract(self, spark: SparkSession) -> DataFrame:

        logger.info(f"Starting Stream Reading [{self.source_format}] from: {self.source_path}")

        initial_schema = DriverSchema.get_spark_schema(
            include_phone=self.include_phone_in_base_schema
        )

        return (
            spark.readStream.format("cloudFiles")
            .schema(initial_schema)
            .option("cloudFiles.format", self.source_format)
            .option("cloudFiles.schemaLocation", f"{self.checkpoint_path}/schema")
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
            .load(self.source_path)
        )

    def load(self, df: DataFrame) -> StreamingQuery:
        logger.info(f"Writing continuous stream to Delta table: {self.full_target_table}")

        query = (
            df.writeStream.format("delta")
            .outputMode("append")
            .option("checkpointLocation", f"{self.checkpoint_path}/data")
            .option("mergeSchema", "true")
            .trigger(processingTime=self.trigger_interval)
            .toTable(self.full_target_table)
        )

        logger.info(f"Stream successfully started. Query ID: {query.id} (Run ID: {query.runId})")
        return query

    def run(self, spark: SparkSession) -> StreamingQuery:
        logger.info(f"Initiating streaming ingestion pipeline for {self.full_target_table}")
        raw_stream_df = self.extract(spark)
        df_with_metadata = self.add_audit_metadata(raw_stream_df)
        return self.load(df_with_metadata)
