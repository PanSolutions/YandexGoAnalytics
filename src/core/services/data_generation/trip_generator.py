from __future__ import annotations

from datetime import timedelta
import random

from pyspark.sql import SparkSession

from src.core.schemas.trip import TaxiTripSchema
from src.core.services.data_generation.base_generator import BaseFileGenerator


class ParquetTaxiTripsGenerator(BaseFileGenerator):

    def __init__(
            self,
            subfolder: str = "taxi_trips",
            volume_name: str = "landing",
            locale: str = "en_US",
    ) -> None:
        super().__init__(
            subfolder=subfolder,
            file_format="parquet",
            volume_name=volume_name,
            locale=locale,
        )

    def generate(self, spark: SparkSession, row_count: int = 1000) -> None:
        schema = TaxiTripSchema.get_spark_schema()
        statuses = ["COMPLETED", "COMPLETED", "COMPLETED", "CANCELLED_BY_USER", "CANCELLED_BY_DRIVER"]
        rows = []

        for _ in range(row_count):
            pickup = self.faker.date_time_this_month()
            duration_minutes = random.randint(5, 55)
            dropoff = pickup + timedelta(minutes=duration_minutes)
            distance = round(random.uniform(1.2, 42.0), 2)
            fare = round(distance * random.uniform(28.0, 48.0) + 149.0, 2)

            rows.append((
                self.faker.uuid4(),
                self.faker.uuid4(),
                self.faker.uuid4(),
                float(distance),
                float(fare),
                pickup,
                dropoff,
                random.choice(statuses),
            ))

        df = spark.createDataFrame(rows, schema=schema)
        df.coalesce(1).write.mode("overwrite").parquet(self.output_path)
