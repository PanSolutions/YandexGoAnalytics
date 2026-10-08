from __future__ import annotations

from pyspark.sql import types as T

from .base import BaseEntitySchema


class UserSchema(BaseEntitySchema):
    @classmethod
    def get_spark_schema(cls) -> T.StructType:
        return T.StructType(
            [
                T.StructField("id", T.StringType(), False),
                T.StructField("full_name", T.StringType(), True),
                T.StructField("email", T.StringType(), True),
                T.StructField("registration_date", T.DateType(), False),
                T.StructField("is_plus_subscriber", T.BooleanType(), False),
            ]
        )
