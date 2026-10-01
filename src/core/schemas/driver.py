from __future__ import annotations

from pyspark.sql import types as T
from src.core.schemas.base import BaseEntitySchema


class DriverSchema(BaseEntitySchema):

    @classmethod
    def get_spark_schema(cls, include_phone: bool = False) -> T.StructType:
        fields = [
            T.StructField("id", T.StringType(), False),
            T.StructField("name", T.StringType(), False),
            T.StructField("car_number", T.StringType(), False),
            T.StructField("experience", T.IntegerType(), False),
            T.StructField("rating", T.DoubleType(), False),
            T.StructField("timestamp", T.TimestampType(), False),
        ]
        if include_phone:
            fields.append(T.StructField("phone", T.StringType(), True))

        return T.StructType(fields)
