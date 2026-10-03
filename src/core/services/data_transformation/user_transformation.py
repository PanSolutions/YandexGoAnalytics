from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.core.services.data_transformation.base_transformation import (
    BaseTransformationService,
)


class UserSilverTransformationService(BaseTransformationService):
    """
    - filter out records where name is null
    - favorite_color null -> "Unknown"
    - add processed_time
    - drop duplicates
    """

    def __init__(self) -> None:
        super().__init__(table_name="users")

    def transform(self, df: DataFrame) -> DataFrame:
        return (
            df.filter(F.col("full_name").isNotNull())
            .withColumn(
                "email",
                F.coalesce(F.col("email"), F.lit("Unknown")),
            )
            .withColumn("_processed_at", F.current_timestamp())
            .dropDuplicates(subset=["email"])
        )
