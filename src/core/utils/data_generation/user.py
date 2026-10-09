from __future__ import annotations

import random
from typing import Any

from faker import Faker
from pyspark.sql import SparkSession

from src.core.schemas.user import UserSchema

from .base import BaseFileGenerator


class UserGenerator(BaseFileGenerator):
    """Generate user records as Avro files.

    A share of rows (``anomaly_rate``) has a null name or email to exercise the
    Silver cleaning rules.
    """

    def __init__(
        self,
        subfolder: str = "users",
        volume_name: str = "landing",
        locale: str = "en_US",
        pool_size: int = 500,
        anomaly_rate: float = 0.06,
    ) -> None:
        """Initialize the generator.

        Args:
            subfolder: Target folder inside the volume.
            volume_name: Unity Catalog volume to write to.
            locale: Faker locale.
            pool_size: Number of distinct user IDs (also the maximum row count).
            anomaly_rate: Probability that a row gets a null name or email.
        """
        super().__init__(
            subfolder=subfolder,
            file_format="avro",
            volume_name=volume_name,
            locale=locale,
        )
        self.pool_size = pool_size
        self.anomaly_rate = anomaly_rate

    @staticmethod
    def get_user_id_pool(pool_size: int = 500) -> list[str]:
        """Return a deterministic pool of user IDs (fixed seed).

        The same pool is used by :class:`TripGenerator`, so generated trips
        reference existing users.

        Args:
            pool_size: Number of IDs.

        Returns:
            A list of UUID strings, identical on every call.
        """
        seeded_faker = Faker()
        seeded_faker.seed_instance(42)
        return [seeded_faker.uuid4() for _ in range(pool_size)]

    def generate(self, spark: SparkSession, row_count: int = 500) -> None:
        """Generate users and append them as an Avro file.

        Each row gets a distinct ID from the pool, so at most ``pool_size`` rows
        are written even if ``row_count`` is larger.

        Args:
            spark: Active Spark session.
            row_count: Requested number of users.
        """
        schema = UserSchema.get_spark_schema()
        user_ids = self.get_user_id_pool(self.pool_size)

        rows: list[tuple[Any, ...]] = []

        for i in range(min(row_count, len(user_ids))):
            name: str | None = self.faker.name()
            email: str | None = self.faker.email()

            if random.random() < self.anomaly_rate:
                anomaly = random.choice(["null_name", "null_email"])
                if anomaly == "null_name":
                    name = None
                elif anomaly == "null_email":
                    email = None

            rows.append(
                (
                    user_ids[i],
                    name,
                    email,
                    self.faker.date_this_decade(),
                    random.choice([True, False]),
                )
            )

        df = spark.createDataFrame(rows, schema=schema)
        df.coalesce(1).write.format("avro").mode("append").save(self.output_path)
