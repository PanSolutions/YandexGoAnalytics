from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .base import BaseTransformationService


class DriverSilverTransformationService(BaseTransformationService):
    """Build ``silver.drivers`` from ``bronze.drivers``.

    Rules:
        - keep rows with ``experience > 0`` and ``rating`` between 1.0 and 5.0
        - add ``_processed_at``
        - drop duplicates by driver ``id``
    """

    def __init__(self) -> None:
        """Bind the service to the ``drivers`` table."""
        super().__init__(table_name="drivers")

    def transform(self, df: DataFrame) -> DataFrame:
        """Filter invalid drivers, add ``_processed_at`` and deduplicate by ``id``."""
        return (
            df.filter(
                (F.col("experience") > 0) & (F.col("rating") >= 1.0) & (F.col("rating") <= 5.0)
            )
            .withColumn("_processed_at", F.current_timestamp())
            .dropDuplicates(subset=["id"])
        )
