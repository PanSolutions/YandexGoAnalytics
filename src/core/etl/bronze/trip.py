from .base import BaseIngestionService


class TaxiTripBatchIngestionService(BaseIngestionService):
    """Ingest taxi trip Parquet files into ``bronze.taxi``."""

    def __init__(
        self,
        source_subfolder: str = "parquet",
        target_table_name: str = "taxi",
    ) -> None:
        """Initialize the service.

        Args:
            source_subfolder: Folder inside the volume that holds the Parquet files.
            target_table_name: Name of the target table.
        """
        super().__init__(
            source_subfolder=source_subfolder,
            target_table_name=target_table_name,
            source_format="parquet",
        )
