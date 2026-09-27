from __future__ import annotations

from pyspark.sql import types as T
from src.core.schemas.base import BaseEntitySchema


class TaxiTripSchema(BaseEntitySchema):

    @classmethod
    def get_spark_schema(cls) -> T.StructType:
        return T.StructType([
            T.StructField("id", T.StringType(), False),
            T.StructField("user_id", T.StringType(), False),
            T.StructField("driver_id", T.StringType(), False),
            T.StructField("distance_km", T.DoubleType(), False),
            T.StructField("fare_amount", T.DoubleType(), False),
            T.StructField("pickup_time", T.TimestampType(), False),
            T.StructField("dropoff_time", T.TimestampType(), False),
            T.StructField("status", T.StringType(), False),
        ])
