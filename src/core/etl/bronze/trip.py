from .base import BaseIngestionService


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
