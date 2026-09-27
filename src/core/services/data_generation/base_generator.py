from __future__ import annotations

from abc import ABC, abstractmethod

from faker import Faker
from pyspark.sql import SparkSession


class BaseFileGenerator(ABC):

    def __init__(self, output_path: str, file_format: str, locale: str = "en_EN") -> None:
        self.faker = Faker(locale)
        self.output_path = output_path
        self.file_format = file_format

    @abstractmethod
    def generate(self, spark: SparkSession, row_count: int) -> None:
        ...

    def __str__(self) -> str:
        return (
            f"{self.__class__.__name__}(path='{self.output_path}',"
            f" format='{self.file_format}')"
        )
