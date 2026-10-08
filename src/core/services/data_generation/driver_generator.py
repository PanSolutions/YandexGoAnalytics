from __future__ import annotations

import random
from datetime import datetime

from faker import Faker
from pyspark.sql import DataFrame, SparkSession

from src.core.schemas.driver import DriverSchema
from src.core.services.data_generation.base_generator import BaseFileGenerator


class DriverGenerator(BaseFileGenerator):
    def __init__(
        self,
        subfolder: str = "drivers_stream",
        volume_name: str = "landing",
        locale: str = "en_US",
        pool_size: int = 100,
        anomaly_rate: float = 0.08,
    ) -> None:
        super().__init__(
            subfolder=subfolder,
            file_format="json",
            volume_name=volume_name,
            locale=locale,
        )
        self.pool_size = pool_size
        self.anomaly_rate = anomaly_rate

    @staticmethod
    def get_driver_id_pool(pool_size: int = 100) -> list[str]:
        seeded_faker = Faker()
        seeded_faker.seed_instance(100)
        return [seeded_faker.uuid4() for _ in range(pool_size)]

    def generate_batch_df(
        self,
        spark: SparkSession,
        row_count: int = 20,
        with_phone: bool = False,
    ) -> DataFrame:
        schema = DriverSchema.get_spark_schema(include_phone=with_phone)
        driver_ids = self.get_driver_id_pool(self.pool_size)

        letters = ["A", "B", "E", "K", "M", "H", "O", "P", "C", "T", "Y", "X"]
        rows = []

        for _ in range(row_count):
            plate = (
                f"{random.choice(letters)}"
                f"{random.randint(100, 999)}"
                f"{random.choice(letters)}{random.choice(letters)}"
            )

            experience = random.randint(1, 25)
            rating = round(random.uniform(4.2, 5.0), 2)

            if random.random() < self.anomaly_rate:
                anomaly = random.choice(["bad_exp", "low_rating", "high_rating"])
                if anomaly == "bad_exp":
                    experience = 0
                elif anomaly == "low_rating":
                    rating = 0.5
                elif anomaly == "high_rating":
                    rating = 5.8

            data = [
                random.choice(driver_ids),
                self.faker.name(),
                plate,
                experience,
                rating,
                datetime.now(),
            ]

            if with_phone:
                data.append(self.faker.phone_number())

            rows.append(tuple(data))

        return spark.createDataFrame(rows, schema=schema)

    def generate(
        self,
        spark: SparkSession,
        row_count: int = 20,
        with_phone: bool = False,
    ) -> None:
        df = self.generate_batch_df(spark, row_count=row_count, with_phone=with_phone)
        df.write.format("json").mode("append").save(self.output_path)
