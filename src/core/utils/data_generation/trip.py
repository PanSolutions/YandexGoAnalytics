from __future__ import annotations

import random
from datetime import timedelta

from pyspark.sql import SparkSession
from user import UserGenerator

from src.core.schemas.trip import TaxiTripSchema

from .base import BaseFileGenerator
from .driver import DriverGenerator


class TripGenerator(BaseFileGenerator):
    def __init__(
        self,
        subfolder: str = "taxi_trips",
        volume_name: str = "landing",
        locale: str = "en_US",
        user_pool_size: int = 500,
        driver_pool_size: int = 100,
        anomaly_rate: float = 0.05,
    ) -> None:
        super().__init__(
            subfolder=subfolder,
            file_format="parquet",
            volume_name=volume_name,
            locale=locale,
        )
        self.user_ids = UserGenerator.get_user_id_pool(user_pool_size)
        self.driver_ids = DriverGenerator.get_driver_id_pool(driver_pool_size)
        self.anomaly_rate = anomaly_rate

    def generate(self, spark: SparkSession, row_count: int = 1000) -> None:
        schema = TaxiTripSchema.get_spark_schema()
        rows = []

        for _ in range(row_count):
            pickup = self.faker.date_time_this_month()
            duration_minutes = random.randint(3, 45)
            dropoff = pickup + timedelta(minutes=duration_minutes)

            distance = round(random.uniform(0.5, 30.0), 2)
            fare = round(distance * random.uniform(2.5, 4.0) + 3.0, 2)
            passengers = random.randint(1, 6)

            if random.random() < self.anomaly_rate:
                anomaly = random.choice(["zero_passengers", "zero_distance", "zero_fare"])
                if anomaly == "zero_passengers":
                    passengers = 0
                elif anomaly == "zero_distance":
                    distance = 0.0
                elif anomaly == "zero_fare":
                    fare = 0.0

            extra = random.choice([0.0, 0.5, 1.0, 2.5])
            mta_tax = 0.5
            tip = round(fare * 0.15, 2) if random.random() > 0.3 else 0.0
            tolls = round(random.uniform(6.5, 13.0), 2) if random.random() > 0.85 else 0.0
            improvement_surcharge = 1.0
            congestion_surcharge = 2.5 if random.random() > 0.2 else 0.0
            airport_fee = 1.25 if random.random() > 0.8 else 0.0

            total = round(
                fare
                + extra
                + mta_tax
                + tip
                + tolls
                + improvement_surcharge
                + congestion_surcharge
                + airport_fee,
                2,
            )

            valid_user_id = random.choice(self.user_ids)
            valid_driver_id = random.choice(self.driver_ids)

            rows.append(
                (
                    self.faker.uuid4(),
                    valid_user_id,
                    valid_driver_id,
                    random.choice([1, 2]),
                    pickup,
                    dropoff,
                    passengers,
                    float(distance),
                    random.choice([1, 1, 1, 2, 3, 4, 5]),
                    random.choice(["N", "Y"]),
                    random.randint(1, 263),
                    random.randint(1, 263),
                    random.choice([1, 1, 2, 3, 4]),
                    float(fare),
                    float(extra),
                    float(mta_tax),
                    float(tip),
                    float(tolls),
                    float(improvement_surcharge),
                    float(total),
                    float(congestion_surcharge),
                    float(airport_fee),
                )
            )

        df = spark.createDataFrame(rows, schema=schema)
        df.coalesce(1).write.mode("append").parquet(self.output_path)
