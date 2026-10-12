"""
Modulo Core
"""

from abc import ABC, abstractmethod
from typing import Dict, Any
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from config.spark_config import SparkSettings

class MedallionStageBase(ABC):
    """
    Abstract class for Medallion Architecture
    """
    def __init__(self, spark: SparkSession, settings: SparkSettings):
        self._spark = spark
        self._settings = settings

    @property
    def spark(self) -> SparkSession:
        return self._spark

    @property
    def settings(self) -> SparkSettings:
        return self._settings

    @abstractmethod
    def read_input(self) -> DataFrame:
        pass

    @abstractmethod
    def transform(self, df: DataFrame) -> DataFrame:
        pass

    @abstractmethod
    def write_output(self, df: DataFrame) -> DataFrame:
        pass

    def execute(self) -> Any:
        print(f"{self.__class__.__name__}")
        input_df = self.read_input()
        transformed_df = self.transform(input_df)
        write_df = self.write_output(transformed_df)
        print(f"{self.__class__.__name__}")
        return write_df