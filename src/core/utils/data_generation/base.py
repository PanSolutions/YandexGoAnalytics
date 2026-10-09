from __future__ import annotations

from abc import ABC, abstractmethod

from faker import Faker
from pyspark.sql import SparkSession

from src.core.config import get_catalog


class BaseFileGenerator(ABC):
    """Base class for generators of synthetic landing files.

    Subclasses generate rows for one entity and write them into
    ``/Volumes/<catalog>/raw_files/<volume_name>/<subfolder>``.
    """

    def __init__(
        self,
        subfolder: str,
        file_format: str,
        volume_name: str = "landing",
        locale: str = "en_US",
    ) -> None:
        """Initialize the Faker instance and the output path.

        Args:
            subfolder: Target folder inside the volume.
            file_format: Output file format (``json``, ``parquet``, ``avro``, ...).
            volume_name: Unity Catalog volume to write to.
            locale: Faker locale used for generated names, emails and phones.
        """
        self.faker = Faker(locale)
        self.file_format = file_format
        self.subfolder = subfolder.strip("/")
        self.volume_name = volume_name

        self.catalog = get_catalog()
        self.schema = "raw_files"

        self.output_path = (
            f"/Volumes/{self.catalog}/{self.schema}/{self.volume_name}/{self.subfolder}"
        )

    @abstractmethod
    def generate(self, spark: SparkSession, row_count: int) -> None:
        """Generate rows and append them as files to :attr:`output_path`.

        Args:
            spark: Active Spark session.
            row_count: Number of rows to generate.
        """

    def __str__(self) -> str:
        """Return a short description with the output path and format."""
        return f"{self.__class__.__name__}(path='{self.output_path}', format='{self.file_format}')"
