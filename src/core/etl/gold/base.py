from __future__ import annotations

from abc import ABC, abstractmethod

from loguru import logger
from pyspark.sql import DataFrame, SparkSession

from src.core.config import get_catalog


class BaseAggregationService(ABC):
    """Aggregate a Silver table into a Gold metrics table.

    Reads ``<catalog>.silver.<source_table_name>`` as a stream, applies
    :meth:`transform_stream` and writes the full aggregate to
    ``<catalog>.gold.<target_table_name>`` in ``complete`` output mode.
    """

    def __init__(
        self,
        source_table_name: str,
        target_table_name: str,
    ) -> None:
        """Initialize table names and the checkpoint path.

        Args:
            source_table_name: Table in the Silver schema.
            target_table_name: Table in the Gold schema.
        """
        self.source_table_name = source_table_name
        self.target_table_name = target_table_name

        self.catalog = get_catalog()
        self.full_source_table = f"{self.catalog}.silver.{self.source_table_name}"
        self.full_target_table = f"{self.catalog}.gold.{self.target_table_name}"

        self.checkpoint_path = (
            f"/Volumes/{self.catalog}/raw_files/landing/_checkpoints/gold/{self.target_table_name}"
        )

    def extract_stream(self, spark: SparkSession) -> DataFrame:
        """Read the Silver table as a stream, ignoring updates to existing files.

        Args:
            spark: Active Spark session.

        Returns:
            A streaming DataFrame over the Silver table.
        """
        logger.info(f"Reading incremental stream from: {self.full_source_table}")
        return (
            spark.readStream.format("delta")
            .option("ignoreChanges", "true")
            .table(self.full_source_table)
        )

    @abstractmethod
    def transform_stream(self, df: DataFrame) -> DataFrame:
        """Aggregate the streaming source into metrics.

        Args:
            df: Streaming DataFrame from the Silver table.

        Returns:
            The aggregated DataFrame to be written to the Gold table.
        """

    def optimize(self, spark: SparkSession) -> None:
        """Compact the target table. Subclasses may override to add clustering.

        Args:
            spark: Active Spark session.
        """
        logger.info(f"Running OPTIMIZE on {self.full_target_table}")
        spark.sql(f"OPTIMIZE {self.full_target_table}")

    def run(self, spark: SparkSession) -> None:
        """Run the aggregation, wait for the query and optimize the target table.

        Args:
            spark: Active Spark session.
        """
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
