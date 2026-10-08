from __future__ import annotations

from abc import ABC, abstractmethod

from faker import Faker
from pyspark.sql import SparkSession

from src.core.config import get_catalog


class BaseFileGenerator(ABC):
    def __init__(
        self,
        subfolder: str,
        file_format: str,
        volume_name: str = "landing",
        locale: str = "en_US",
    ) -> None:
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
    def generate(self, spark: SparkSession, row_count: int) -> None: ...

    def __str__(self) -> str:
        return f"{self.__class__.__name__}(path='{self.output_path}', format='{self.file_format}')"
