from __future__ import annotations

from abc import ABC, abstractmethod

from loguru import logger
from pyspark.sql import DataFrame, SparkSession

from src.core.config import get_catalog


class BaseTransformationService(ABC):
    """Incrementally transform a Bronze table into a Silver table.

    Reads new rows from ``<catalog>.<source_schema>.<table_name>`` as a stream,
    applies :meth:`transform` and appends the result to
    ``<catalog>.<target_schema>.<table_name>``.
    """

    def __init__(
        self,
        table_name: str,
        source_schema: str = "bronze",
        target_schema: str = "silver",
    ) -> None:
        """Initialize table names and the checkpoint path.

        Args:
            table_name: Table name, the same in the source and target schemas.
            source_schema: Schema to read from.
            target_schema: Schema to write to.
        """
        self.table_name = table_name
        self.source_schema = source_schema
        self.target_schema = target_schema

        self.catalog = get_catalog()
        self.full_source_table = f"{self.catalog}.{self.source_schema}.{self.table_name}"
        self.full_target_table = f"{self.catalog}.{self.target_schema}.{self.table_name}"

        self.checkpoint_path = (
            f"/Volumes/{self.catalog}/raw_files/landing/_checkpoints/"
            f"{self.target_schema}/{self.table_name}"
        )

    def extract(self, spark: SparkSession) -> DataFrame:
        """Read the source table as a stream, ignoring updates to existing files.

        Args:
            spark: Active Spark session.

        Returns:
            A streaming DataFrame over the source table.
        """
        logger.info(f"Reading incremental stream from Bronze: {self.full_source_table}")
        return (
            spark.readStream.format("delta")
            .option("ignoreChanges", "true")
            .table(self.full_source_table)
        )

    @abstractmethod
    def transform(self, df: DataFrame) -> DataFrame:
        """Clean and enrich the source rows.

        Args:
            df: Streaming DataFrame from the source table.

        Returns:
            The transformed DataFrame to be written to the target table.
        """

    def load(self, df: DataFrame) -> None:
        """Append the stream to the target table and wait for completion.

        The query runs with ``availableNow=True``: it processes the new rows and stops.

        Args:
            df: Transformed streaming DataFrame.
        """
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
        """Run the full transformation: extract, transform, load.

        Args:
            spark: Active Spark session.
        """
        logger.info(
            f"Starting incremental transformation: {self.full_source_table} -> {self.full_target_table}"
        )
        new_bronze_rows = self.extract(spark)
        cleansed_df = self.transform(new_bronze_rows)
        self.load(cleansed_df)
