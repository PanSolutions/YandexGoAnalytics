from __future__ import annotations

from abc import ABC, abstractmethod
from typing import ClassVar

from pyspark.sql import types as T


class BaseEntitySchema(ABC):
    """Base class for entity schemas.

    Subclasses describe the business columns of one entity in
    :meth:`get_spark_schema`. Technical ingestion columns are appended by
    :meth:`get_spark_schema_with_metadata`.
    """

    METADATA_FIELDS: ClassVar[list[T.StructField]] = [
        T.StructField("_ingested_at", T.TimestampType(), False),
        T.StructField("_source_file", T.StringType(), True),
    ]
    """Technical columns added to every entity at ingestion time."""

    @classmethod
    @abstractmethod
    def get_spark_schema(cls) -> T.StructType:
        """Return the Spark schema with the entity's business columns."""

    @classmethod
    def get_spark_schema_with_metadata(cls) -> T.StructType:
        """Return the entity schema extended with :attr:`METADATA_FIELDS`."""
        base_schema = cls.get_spark_schema()
        return T.StructType(base_schema.fields + cls.METADATA_FIELDS)
