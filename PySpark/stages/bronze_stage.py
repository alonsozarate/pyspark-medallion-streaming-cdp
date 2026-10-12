"""
Bronze Layer
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from core.base_stage import MedallionStageBase

class BronzeKafkaIngestionStage(MedallionStageBase):
    def __init__(self, spark, settings, kafka_bootstrap_servers: str, kafka_topic: str):
        super().__init__(spark, settings)
        self._kafka_bootstrap_servers = kafka_bootstrap_servers
        self._kafka_topic = kafka_topic

        # Roots
        self._target_delta_path = f"{self.settings.base_storage_path}/bronze/kafka_events_raw"
        self._checkpoint_path = f"{self.settings.base_storage_path}/_checkpoints/bronze_kafka"

    def _get_event_schema(self) -> StructType:
        return StructType([
            StructField("transaction_id", StringType(), True),
            StructField("user_id", StringType(), True),
            StructField("amount", DoubleType(), True),
            StructField("event_type", StringType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("country_code", StringType(), True),
            # Corrupted JSON
            StructField("_corrupt_record", StringType(), True)
        ])

    def read_input(self) -> DataFrame:
        return self.spark.readStream\
            .format("kafka")\
            .option("kafka.bootstrap.servers", self._kafka_servers)\
            .option("subscribe", self._kafka_topic)\
            .option("startingOffsets","latest")\
            .option("failOnDataLoss", "false")\
            .load()


    def transform(self, df: DataFrame) -> DataFrame:
        schema = self._get_event_schema()

        return df.select(
            F.col("key").cast("string").alias("kafka_key"),
            F.from_json(
                F.col("value").cast("string"),
                schema,
                {
                    "mode":"PERMISSIVE",
                    "columnNameOfCorruptRecord":"_corrupt_record"
                }
            ).alias("payload"),
            F.col("topic").alias("kafka_topic"),
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
            F.col("timestamp").alias("kafka_timestamp"),
        ).select(
            "payload.transaction_id",
            "payload.user_id",
            "payload.amount",
            "payload.event_type",
            "payload.timestamp",
            "payload.country_code",
            "payload._corrupt_record", 
            "kafka_key",
            "kafka_topic",
            "kafka_partition",
            "kafka_offset",
            "kafka_timestamp",
            F.current_timestamp().alias("ingestion_timestamp")
        )

    def write_output(self, df: DataFrame) -> DataFrame:
        return df.writeStream\
            .format("delta")\
            .outputMode("append")\
            .option("checkpointLocation", self._checkpoint_path)\
            .start(self._target_delta_path)