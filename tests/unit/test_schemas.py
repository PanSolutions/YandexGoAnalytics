from __future__ import annotations

from pyspark.sql import types as T

from src.core.schemas.driver import DriverSchema
from src.core.schemas.trip import TaxiTripSchema
from src.core.schemas.user import UserSchema


class TestEntitySchemas:
    """Test suite validating Spark StructType schema definitions and field additions."""

    def test_driver_schema_without_phone(self) -> None:
        """Ensure DriverSchema returns core fields when include_phone=False."""
        schema = DriverSchema.get_spark_schema(include_phone=False)
        field_names = schema.fieldNames()
        assert "id" in field_names
        assert "rating" in field_names
        assert "phone" not in field_names

    def test_driver_schema_with_phone(self) -> None:
        """Ensure DriverSchema appends phone column when requested."""
        schema = DriverSchema.get_spark_schema(include_phone=True)
        assert "phone" in schema.fieldNames()

    def test_schema_with_metadata(self) -> None:
        """Validate metadata fields append properly to BaseEntitySchema."""
        schema = DriverSchema.get_spark_schema_with_metadata()
        field_names = schema.fieldNames()
        assert "_ingested_at" in field_names
        assert "_source_file" in field_names
        assert isinstance(schema["_ingested_at"].dataType, T.TimestampType)

    def test_taxi_trip_schema(self) -> None:
        """Validate TaxiTripSchema fields and structural types."""
        schema = TaxiTripSchema.get_spark_schema()
        field_names = schema.fieldNames()
        assert "fare_amount" in field_names
        assert "pu_location_id" in field_names
        assert isinstance(schema["passenger_count"].dataType, T.LongType)

    def test_user_schema(self) -> None:
        """Validate UserSchema structure."""
        schema = UserSchema.get_spark_schema()
        field_names = schema.fieldNames()
        assert "full_name" in field_names
        assert "is_plus_subscriber" in field_names
        assert isinstance(schema["registration_date"].dataType, T.DateType)
