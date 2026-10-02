from __future__ import annotations

from src.core.services.data_ingestion.base import BaseIngestionService


class UserBatchIngestionService(BaseIngestionService):

    def __init__(
        self,
        source_subfolder: str = "avro",
        target_table_name: str = "users",
    ) -> None:
        super().__init__(
            source_subfolder=source_subfolder,
            target_table_name=target_table_name,
            source_format="avro",
        )


class TaxiTripBatchIngestionService(BaseIngestionService):

    def __init__(
        self,
        source_subfolder: str = "parquet",
        target_table_name: str = "taxi",
    ) -> None:
        super().__init__(
            source_subfolder=source_subfolder,
            target_table_name=target_table_name,
            source_format="parquet",
        )
