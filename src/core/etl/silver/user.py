from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .base import BaseTransformationService


class UserSilverTransformationService(BaseTransformationService):
    """Build ``silver.users`` from ``bronze.users``.

    Rules:
        - drop rows where ``full_name`` is null
        - replace a null ``email`` with ``"Unknown"``
        - add ``_processed_at``
        - drop duplicates by ``email``. Null emails are replaced first, so all
          such users share the value ``"Unknown"`` and collapse into one row.
    """

    def __init__(self) -> None:
        """Bind the service to the ``users`` table."""
        super().__init__(table_name="users")

    def transform(self, df: DataFrame) -> DataFrame:
        """Drop unnamed users, fill missing emails and deduplicate by ``email``."""
        return (
            df.filter(F.col("full_name").isNotNull())
            .withColumn(
                "email",
                F.coalesce(F.col("email"), F.lit("Unknown")),
            )
            .withColumn("_processed_at", F.current_timestamp())
            .dropDuplicates(subset=["email"])
        )
