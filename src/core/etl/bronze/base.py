from __future__ import annotations

from loguru import logger
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery

from src.core.config import get_catalog


class BaseIngestionService:
    """Incrementally ingest files from a landing volume into a Bronze Delta table.

    Reads files with Auto Loader (``cloudFiles``), adds audit columns and appends
    the result to ``<catalog>.<target_schema>.<target_table_name>``. Subclasses
    usually only set the source folder, format and target table.
    """

    def __init__(
        self,
        source_subfolder: str,
        target_table_name: str,
        source_format: str,
        volume_name: str = "landing",
        target_schema: str = "bronze",
    ) -> None:
        """Initialize paths and table names.

        Args:
            source_subfolder: Folder inside the volume that holds the source files.
            target_table_name: Name of the target table.
            source_format: Auto Loader file format (``parquet``, ``json``, ``avro``, ...).
            volume_name: Unity Catalog volume with the raw files.
            target_schema: Schema of the target table.
        """
        self.source_format = source_format
        self.source_subfolder = source_subfolder.strip("/")
        self.volume_name = volume_name
        self.target_schema = target_schema
        self.target_table_name = target_table_name

        self.catalog = get_catalog()

        self.source_path = (
            f"/Volumes/{self.catalog}/raw_files/{self.volume_name}/{self.source_subfolder}"
        )
        self.checkpoint_path = (
            f"/Volumes/{self.catalog}/raw_files/{self.volume_name}/_checkpoints/"
            f"{self.target_schema}/{self.target_table_name}"
        )
        self.full_target_table = f"{self.catalog}.{self.target_schema}.{self.target_table_name}"

    def extract(self, spark: SparkSession) -> DataFrame:
        """Create a streaming DataFrame over the source files.

        Args:
            spark: Active Spark session.

        Returns:
            A streaming DataFrame that picks up new and existing files and adds
            new columns to the schema as they appear.
        """
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
        """Add ``_ingested_at`` and ``_source_file`` columns.

        Args:
            df: Source DataFrame read with Auto Loader.

        Returns:
            The DataFrame with the audit columns.
        """
        return df.withColumn("_ingested_at", F.current_timestamp()).withColumn(
            "_source_file", F.col("_metadata.file_path")
        )

    def load(self, df: DataFrame) -> StreamingQuery | None:
        """Write the stream to the target Delta table and wait for completion.

        The query runs with ``availableNow=True``: it processes everything that is
        available and stops.

        Args:
            df: DataFrame to write.

        Returns:
            ``None`` in the base implementation; subclasses may return the query.
        """
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
        """Run the full ingestion: extract, add audit metadata, load.

        Args:
            spark: Active Spark session.

        Returns:
            The result of :meth:`load`.
        """
        logger.info(f"Start data ingestion for {self.full_target_table}")
        raw_df = self.extract(spark)
        df_with_metadata = self.add_audit_metadata(raw_df)
        self.load(df_with_metadata)
        return None
