from __future__ import annotations

from loguru import logger
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.streaming import StreamingQuery

from src.core.schemas import DriverSchema

from .base import BaseIngestionService


class DriverStreamingIngestionService(BaseIngestionService):
    """Ingest the drivers JSON stream into ``bronze.drivers``.

    Unlike the base class, the Auto Loader read uses :class:`DriverSchema` as
    schema hints, and the write returns the finished streaming query.
    """

    def __init__(
        self,
        source_subfolder: str = "json",
        target_table_name: str = "drivers",
        source_format: str = "json",
        trigger_interval: str = "2 seconds",
        include_phone_in_base_schema: bool = False,
    ) -> None:
        """Initialize the service.

        Args:
            source_subfolder: Folder inside the volume that holds the JSON files.
            target_table_name: Name of the target table.
            source_format: Auto Loader file format.
            trigger_interval: Stored for configuration only. The query currently
                runs with ``availableNow=True`` and does not use it.
            include_phone_in_base_schema: Include the ``phone`` column in the schema hints.
        """
        super().__init__(
            source_subfolder=source_subfolder,
            target_table_name=target_table_name,
            source_format=source_format,
        )
        self.trigger_interval = trigger_interval
        self.include_phone_in_base_schema = include_phone_in_base_schema

    def extract(self, spark: SparkSession) -> DataFrame:
        """Create a streaming DataFrame with driver schema hints.

        Args:
            spark: Active Spark session.

        Returns:
            A streaming DataFrame over the driver files.
        """
        logger.info(f"Starting Stream Reading [{self.source_format}] from: {self.source_path}")

        initial_schema = DriverSchema.get_spark_schema(
            include_phone=self.include_phone_in_base_schema
        )

        schema_hints = initial_schema.toDDL()

        return (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", self.source_format)
            .option("cloudFiles.schemaLocation", f"{self.checkpoint_path}/schema")
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
            .option("cloudFiles.schemaHints", schema_hints)
            .load(self.source_path)
        )

    def load(self, df: DataFrame) -> StreamingQuery:
        """Append the stream to the target table and wait for completion.

        Args:
            df: DataFrame to write.

        Returns:
            The finished streaming query.
        """
        logger.info(f"Writing continuous stream to Delta table: {self.full_target_table}")

        query = (
            df.writeStream.format("delta")
            .outputMode("append")
            .option("checkpointLocation", f"{self.checkpoint_path}/data")
            .option("mergeSchema", "true")
            .trigger(availableNow=True)
            .toTable(self.full_target_table)
        )

        query.awaitTermination()

        logger.info(f"Stream batch processed successfully. Query ID: {query.id}")
        return query

    def run(self, spark: SparkSession) -> StreamingQuery:
        """Run the full ingestion: extract, add audit metadata, load.

        Args:
            spark: Active Spark session.

        Returns:
            The finished streaming query.
        """
        logger.info(f"Initiating streaming ingestion pipeline for {self.full_target_table}")
        raw_stream_df = self.extract(spark)
        df_with_metadata = self.add_audit_metadata(raw_stream_df)
        return self.load(df_with_metadata)
