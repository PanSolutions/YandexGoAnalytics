from __future__ import annotations

from loguru import logger
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.core.config import get_catalog


class EnrichedTripsGoldService:
    def __init__(self) -> None:
        self.catalog = get_catalog()
        self.full_target_table = f"{self.catalog}.gold.enriched_trips"
        self.checkpoint_path = (
            f"/Volumes/{self.catalog}/raw_files/landing/_checkpoints/gold/enriched_trips"
        )

    def run(self, spark: SparkSession) -> None:
        logger.info("Reading stream from silver.taxi via checkpoints...")

        stream_taxi = (
            spark.readStream.format("delta")
            .option("ignoreChanges", "true")
            .table(f"{self.catalog}.silver.taxi")
            .dropDuplicates(["id"])
        )

        static_drivers = spark.table(f"{self.catalog}.silver.drivers").dropDuplicates(["id"])

        driver_columns = [
            F.col("id").alias("driver_id"),
            F.col("name").alias("driver_name"),
            F.col("car_number").alias("driver_car_number"),
            F.col("experience").alias("driver_experience"),
            F.col("rating").alias("driver_rating"),
        ]
        if "phone" in static_drivers.columns:
            driver_columns.append(F.col("phone").alias("driver_phone"))

        clean_drivers = static_drivers.select(*driver_columns)

        enriched_stream = stream_taxi.join(clean_drivers, on="driver_id", how="left").withColumn(
            "_calculated_at", F.current_timestamp()
        )

        query = (
            enriched_stream.writeStream.format("delta")
            .outputMode("append")
            .option("checkpointLocation", f"{self.checkpoint_path}/data")
            .option("mergeSchema", "true")
            .trigger(availableNow=True)
            .toTable(self.full_target_table)
        )

        query.awaitTermination()

        spark.sql(f"OPTIMIZE {self.full_target_table}")
        logger.info("Enriched yandex_go_trips_pipeline successfully written!")
