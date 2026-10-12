from typing import Dict, Any
from pyspark.sql import SparkSession


class SparkSettings:
    def __init__(self, app_name:str = "Enterprise_Medallion_Streaming"):
        self._app_name = app_name
        self._base_storage_path = "abfss://data-lake@storageaccount.dfs.core.windows.net"

    @property
    def base_storage_path(self) -> str:
        return self._base_storage_path

    def get_spark_configuration(self) -> Dict[str,str]:
        return {
            # AQE (Adaptive Query Execution)
            "spark.sql.adaptive.enabled":"true",
            "spark.sql.adaptive.coalescePartitions.enabled":"true",
            # High number 200
            "spark.sql.adaptative.coalescePartitions.initialPartitionNum":"200",
            "spark.sql.adaptative.skewJoin.enabled":"true",
            # skewed if > partition x 5
            "spark.sql.adaptative.skewJoin.skewedPartitionFactor":"5",
            "spark.sql.adaptative.skewJoin.skewedPartitionThresholdInBytes":"256MB",

            # JOIN & MEMORY
            "spark.sql.autoBroadcastJoinThreshold": str(20 * 1024 * 1024), # 20MB
            "spark.sql.execution.arrow.pyspark.enabled":"true",
            "spark.sql.shuffle.partitions":"200",

            # DELTA LAKE OPTIMIZATION
            "spark.databricks.delta.optimizeWrite.enabled":"true",
            "spark.databricks.delta.autoCompact.enabled":"true",
            "spark.databricks.delta.vacuum.parallelDelete.enabled":"true",

            # MEMORY MANAGMENT
            "spark.memory.fraction":"0.6",
            "spark.memory.storageFraction":"0.5"
        }

class SparkSessionFactory:
    @staticmethod
    def create_session(settings: SparkSettings) -> SparkSession:
        builder = SparkSession.builder.appName(settings._app_name)

        for key, value in settings.get_spark_configuration().items():
            builder = builder.config(key, value)

        spark = builder.getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
        return spark