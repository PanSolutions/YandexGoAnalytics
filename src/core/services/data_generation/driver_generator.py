from __future__ import annotations

from datetime import datetime
import random

from pyspark.sql import DataFrame, SparkSession

from src.core.services.data_generation.base_generator import BaseFileGenerator
from src.core.schemas.driver import DriverSchema


class JsonDriversGenerator(BaseFileGenerator):

    def __init__(
            self,
            subfolder: str = "drivers_stream",
            volume_name: str = "landing",
            locale: str = "en_US",
    ) -> None:
        super().__init__(
            subfolder=subfolder,
            file_format="json",
            volume_name=volume_name,
            locale=locale,
        )

    def generate_batch_df(
        self,
        spark: SparkSession,
        row_count: int = 15,
        with_phone: bool = False,
    ) -> DataFrame:
        """Создает DataFrame водителей на лету."""
        schema = DriverSchema.get_spark_schema(include_phone=with_phone)
        letters = ["А", "В", "Е", "К", "М", "Н", "О", "Р", "С", "Т", "У", "Х"]
        rows = []

        for _ in range(row_count):
            plate = (
                f"{random.choice(letters)}"
                f"{random.randint(100, 999)}"
                f"{random.choice(letters)}{random.choice(letters)} "
                f"{random.choice(['77', '99', '199', '777'])}"
            )

            data = [
                self.faker.name(),
                plate,
                random.randint(1, 20),
                round(random.uniform(4.3, 5.0), 2),
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
