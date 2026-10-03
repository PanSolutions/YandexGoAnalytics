from __future__ import annotations

import random

from faker import Faker
from pyspark.sql import SparkSession

from src.core.schemas.user import UserSchema
from src.core.services.data_generation.base_generator import BaseFileGenerator


class UserGenerator(BaseFileGenerator):
    def __init__(
        self,
        subfolder: str = "users",
        volume_name: str = "landing",
        locale: str = "en_US",
        pool_size: int = 500,
    ) -> None:
        super().__init__(
            subfolder=subfolder,
            file_format="avro",
            volume_name=volume_name,
            locale=locale,
        )
        self.pool_size = pool_size

    @staticmethod
    def get_user_id_pool(pool_size: int = 500) -> list[str]:
        seeded_faker = Faker()
        seeded_faker.seed_instance(42)
        return [seeded_faker.uuid4() for _ in range(pool_size)]

    def generate(self, spark: SparkSession, row_count: int = 500) -> None:
        schema = UserSchema.get_spark_schema()
        user_ids = self.get_user_id_pool(self.pool_size)

        rows = []
        for i in range(min(row_count, len(user_ids))):
            rows.append(
                (
                    user_ids[i],
                    self.faker.name(),
                    self.faker.email(),
                    self.faker.date_this_decade(),
                    random.choice([True, False]),
                )
            )

        df = spark.createDataFrame(rows, schema=schema)
        df.coalesce(1).write.format("avro").mode("append").save(self.output_path)
