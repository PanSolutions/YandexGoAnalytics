from __future__ import annotations

try:
    import dlt
except ImportError:
    from unittest.mock import MagicMock

    dlt = MagicMock()

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()

CATALOG = spark.conf.get("pipeline.catalog")
LANDING_PATH = f"/Volumes/{CATALOG}/raw_files/landing"


class BronzeTableBuilder:
    def __init__(
        self,
        table_name: str,
        subfolder: str,
        source_format: str,
        enable_schema_evolution: bool = False,
    ) -> None:
        self.table_name = table_name
        self.source_path = f"{LANDING_PATH}/{subfolder}"
        self.source_format = source_format
        self.enable_schema_evolution = enable_schema_evolution

    def build(self) -> None:

        @dlt.table(
            name=self.table_name,
            comment=f"Raw streaming ingestion for {self.table_name}",
        )
        def _table_func():
            reader = spark.readStream.format("cloudFiles").option(
                "cloudFiles.format", self.source_format
            )
            if self.enable_schema_evolution:
                reader = reader.option("cloudFiles.schemaEvolutionMode", "addNewColumns")

            return (
                reader.load(self.source_path)
                .withColumn("_ingested_at", F.current_timestamp())
                .withColumn("_source_file", F.col("_metadata.file_path"))
            )


BronzeTableBuilder("bronze_taxi", "parquet", "parquet").build()
BronzeTableBuilder("bronze_users", "avro", "avro").build()
BronzeTableBuilder("bronze_drivers", "json", "json", enable_schema_evolution=True).build()
