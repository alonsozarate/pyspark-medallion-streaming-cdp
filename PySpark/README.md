# PySpark Medallion Streaming Pipeline (CDP & PII Compliance)

This repository demonstrates a production-ready PySpark Structured Streaming pipeline designed for Customer Data Platforms (CDP). It processes Kafka event streams through a Medallion Architecture (Bronze, Silver, Gold) on Azure Databricks.

## Architecture Highlights
* **Bronze Layer:** Ingests raw JSON payloads from Apache Kafka handling schema drift via `PERMISSIVE` mode.
* **Silver Layer:** Implements a robust Dead Letter Queue (DLQ) pattern to quarantine malformed records and a dynamic PII masking framework (SHA-256) for GDPR compliance.
* **Gold Layer:** Performs stateful aggregations using **Salting techniques** to mitigate data skew and Delta Lake `MERGE` operations for idempotent upserts.

## Engineering Practices
* **Object-Oriented Programming (OOP):** Abstract Base Classes (`ABC`) and the Template Method pattern are used to ensure DRY principles and modularity across pipeline stages.
* **Spark Optimization:** Adaptive Query Execution (AQE), Dynamic Partition Pruning, and `OPTIMIZE ZORDER BY` are configured for performance tuning.