from .base import BaseIngestionService


class UserBatchIngestionService(BaseIngestionService):
    """Ingest user Avro files into ``bronze.users``."""

    def __init__(
        self,
        source_subfolder: str = "avro",
        target_table_name: str = "users",
    ) -> None:
        """Initialize the service.

        Args:
            source_subfolder: Folder inside the volume that holds the Avro files.
            target_table_name: Name of the target table.
        """
        super().__init__(
            source_subfolder=source_subfolder,
            target_table_name=target_table_name,
            source_format="avro",
        )
