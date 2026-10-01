from __future__ import annotations

import random

from pyspark.sql import SparkSession

from src.core.schemas.user import UserSchema
from src.core.services.data_generation.base_generator import BaseFileGenerator


class AvroUsersGenerator(BaseFileGenerator):

    def __init__(
            self,
            subfolder: str = "users",
            volume_name: str = "landing",
            locale: str = "en_US",
    ) -> None:
        super().__init__(
            subfolder=subfolder,
            file_format="avro",
            volume_name=volume_name,
            locale=locale,
        )

    def generate(self, spark: SparkSession, row_count: int = 500) -> None:
        schema = UserSchema.get_spark_schema()
        rows = []

        for _ in range(row_count):
            rows.append((
                self.faker.uuid4(),
                self.faker.name(),
                self.faker.email(),
                self.faker.date_this_decade(),
                random.choice([True, False]),
            ))

        df = spark.createDataFrame(rows, schema=schema)
        df.coalesce(1).write.format("avro").mode("overwrite").save(self.output_path)
