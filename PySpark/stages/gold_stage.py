from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from delta.tables import DeltaTable
from core.base_stage import MedallionStageBase


class GoldAggregationStage(MedallionStageBase):

    def __init__(self, spark, settings):
        super().__init__(spark, settings)
        self._source_silver = f"{self.settings.base_storage_path}/silver/transactions_cleaned"
        self._target_gold = f"{self.settings.base_storage_path}/gold/kpi_daily_country_metrics"
        
        # Spark Accumulator
        self.invalid_records_acc = self.spark.sparkContext.accumulator(0)

    def read_input(self) -> DataFrame:
        return self.spark.read.format("delta").load(self._source_silver)

    def _get_salted_dim_countries(self, num_salts: int) -> DataFrame:
        data = [("PE", "Peru", "LATAM"), ("CO", "Colombia", "LATAM"), ("MX", "Mexico", "LATAM")]
        df_dim = self.spark.createDataFrame(data, ["country_code", "country_name", "region"])
        
        array_salts = F.array([F.lit(i) for i in range(num_salts)])
        return df_dim.withColumn("salt_key", F.explode(array_salts))

    def transform(self, df: DataFrame) -> DataFrame:
        """
        Data Skew
        """
        NUM_SALTS = 4
        
        df_salted_facts = df.withColumn("salt_key", F.floor(F.rand() * NUM_SALTS))
        
        df_salted_dim = self._get_salted_dim_countries(NUM_SALTS)
        
        df_joined = df_salted_facts.join(
            F.broadcast(df_salted_dim),
            (df_salted_facts["country_code"] == df_salted_dim["country_code"]) &
            (df_salted_facts["salt_key"] == df_salted_dim["salt_key"]),
            "inner"
        )

        return df_joined.groupBy(
            "region",
            "country_name",
            F.to_date("event_timestamp").alias("metric_date")
        ).agg(
            F.count("transaction_id").alias("total_transactions"),
            F.sum("amount_usd").alias("total_volume_usd"),
            F.avg("amount_usd").alias("avg_ticket_usd"),
            F.max("ingestion_timestamp").alias("last_processed_at")
        )

    def write_output(self, df_gold_aggregated: DataFrame):   
        df_gold_aggregated.write \
            .format("delta") \
            .mode("ignore") \
            .partitionBy("metric_date") \
            .save(self._target_gold)

        delta_table_gold = DeltaTable.forPath(self.spark, self._target_gold)

        # MERGE (Upsert)
        delta_table_gold.alias("target").merge(
            df_gold_aggregated.alias("source"),
            """
            target.region = source.region AND
            target.country_name = source.country_name AND
            target.metric_date = source.metric_date
            """
        ).whenMatchedUpdate(set={
            "total_transactions": "source.total_transactions",
            "total_volume_usd": "source.total_volume_usd",
            "avg_ticket_usd": "source.avg_ticket_usd",
            "last_processed_at": "source.last_processed_at"
        }).whenNotMatchedInsertAll().execute()

        
        self.spark.sql(f"OPTIMIZE delta.`{self._target_gold}` ZORDER BY (region, country_name)")