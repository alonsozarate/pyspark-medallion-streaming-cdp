"""
Silver Layer
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from core.base_stage import MedallionStageBase

class SilverCleansingStage(MedallionStageBase):
    def __init__(self, spark, settings):
        super().__init__(spark, settings)
        self._source_delta_path = f"{self.settings.base_storage_path}/bronze/kafka_events_raw"
        self._target_silver_path = f"{self.settings.base_storage_path}/silver/transactions_cleaned"
        self._quarantine_path = f"{self.settings.base_storage_path}/quarantine/corrupted_transactions"

        self._checkpoint_silver = f"{self.settings.base_storage_path}/_checkpoints/silver_cleansing"
        self._checkpoint_quarantine = f"{self.settings.base_storage_path}/_checkpoints/quarantine_silver"


    def read_input(self) -> DataFrame:
        return self.spark.readStream\
            .format("delta")\
            .load(self._source_delta_path)

    def transform(self, df: DataFrame) -> DataFrame:
        """Aplica reglas de negocio y parseo de tipo de datos"""
        return df.withColumn(
            "event_timestamp", F.to_timestamp(F.col("timestamp"))
        ).withColumn(
            "amount_usd", F.round(F.col("amount") * F.lit(1.0), 2)
        )

    def write_output(self, df: DataFrame):
        # Dead letter queue
        df_corrupted = df.filter(
            (F.col("_corrupt_record").isNotNull()) |
            (F.col("transaction_id").isNull()) |
            (F.col("amount").isNull()) |
            (F.col("amount") <= 0)
        )

        df_valid = df.filter(
            (F.col("_corrupt_record").isNull()) &
            (F.col("transaction_id").isNotNull()) &
            (F.col("amount") > 0)
        ).dropDuplicates(['transaction_id'])

        q_quarantine = df_corrupted.writeStream\
            .format("delta")\
            .outputMode("append")\
            .option("checkpointLocation", self._checkpoint_quarantine)\
            .trigger(availableNow=True)\
            .start(self._quarantine_path)

        q_silver = df_valid.writeStream\
            .format("delta")\
            .outputMode("append")\
            .option("checkpointLocation", self._checkpoint_silver)\
            .trigger(availableNow=True)\
            .start(self._target_silver_path)

        return q_silver