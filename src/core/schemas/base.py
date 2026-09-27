from __future__ import annotations

from abc import ABC, abstractmethod
from typing import ClassVar

from pyspark.sql import types as T


class BaseEntitySchema(ABC):

    METADATA_FIELDS: ClassVar[list[T.StructField]] = [
        T.StructField("_ingested_at", T.TimestampType(), False),
        T.StructField("_source_file", T.StringType(), True),
    ]

    @classmethod
    @abstractmethod
    def get_spark_schema(cls) -> T.StructType:
        ...

    @classmethod
    def get_spark_schema_with_metadata(cls) -> T.StructType:
        base_schema = cls.get_spark_schema()
        return T.StructType(base_schema.fields + cls.METADATA_FIELDS)
