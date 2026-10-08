from __future__ import annotations

from pyspark.sql import types as T

from .base import BaseEntitySchema


class TaxiTripSchema(BaseEntitySchema):
    @classmethod
    def get_spark_schema(cls) -> T.StructType:
        return T.StructType(
            [
                T.StructField("id", T.StringType(), False),
                T.StructField("user_id", T.StringType(), False),
                T.StructField("driver_id", T.StringType(), False),
                T.StructField("vendor_id", T.IntegerType(), True),
                T.StructField("pickup_datetime", T.TimestampType(), True),
                T.StructField("dropoff_datetime", T.TimestampType(), True),
                T.StructField("passenger_count", T.LongType(), True),
                T.StructField("trip_distance", T.DoubleType(), True),
                T.StructField("rate_code_id", T.LongType(), True),
                T.StructField("store_and_fwd_flag", T.StringType(), True),
                T.StructField("pu_location_id", T.IntegerType(), True),
                T.StructField("do_location_id", T.IntegerType(), True),
                T.StructField("payment_type", T.LongType(), True),
                T.StructField("fare_amount", T.DoubleType(), True),
                T.StructField("extra", T.DoubleType(), True),
                T.StructField("mta_tax", T.DoubleType(), True),
                T.StructField("tip_amount", T.DoubleType(), True),
                T.StructField("tolls_amount", T.DoubleType(), True),
                T.StructField("improvement_surcharge", T.DoubleType(), True),
                T.StructField("total_amount", T.DoubleType(), True),
                T.StructField("congestion_surcharge", T.DoubleType(), True),
                T.StructField("airport_fee", T.DoubleType(), True),
            ]
        )
