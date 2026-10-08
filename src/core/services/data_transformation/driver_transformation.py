from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.core.services.data_transformation.base_transformation import (
    BaseTransformationService,
)


class DriverSilverTransformationService(BaseTransformationService):
    """
    - experience > 0, rating between 1.0 and 5.0
    - add processed_time
    - drop duplicates based on driver's ID
    """

    def __init__(self) -> None:
        super().__init__(table_name="drivers")

    def transform(self, df: DataFrame) -> DataFrame:
        return (
            df.filter(
                (F.col("experience") > 0) & (F.col("rating") >= 1.0) & (F.col("rating") <= 5.0)
            )
            .withColumn("_processed_at", F.current_timestamp())
            .dropDuplicates(subset=["id"])
        )
