"""
Entry Point Principal
"""

from typing import List
from config.spark_config import SparkSettings, SparkSessionFactory
from core.base_stage import MedallionStageBase
from stages.bronze_stage import BronzeKafkaIngestionStage
from stages.silver_stage import SilverCleansingStage
from stages.gold_stage import GoldAggregationStage


def main():
    settings = SparkSettings(app_name="Databricks_Medallion_Production_Pipeline")
    spark = SparkSessionFactory.create_session(settings)

    KAFKA_BOOTSTRAP_SERVERS = "10.0.0.4:9092,10.0.0.5:9092"
    KAFKA_TOPIC = "telemetry-events"

    bronze_stage = BronzeKafkaIngestionStage(
        spark, settings, KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC
    )
    silver_stage = SilverCleansingStage(spark, settings)
    gold_stage = GoldAggregationStage(spark, settings)

    pipeline_stages: List[MedallionStageBase] = [
        bronze_stage,
        silver_stage,
        gold_stage
    ]

    for stage in pipeline_stages:
        query_handle = stage.execute()
        if query_handle and hasattr(query_handle, "awaitTermination"):
            query_handle.awaitTermination()


if __name__ == "__main__":
    main()