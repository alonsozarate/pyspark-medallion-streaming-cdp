# PySpark Medallion Streaming Pipeline (CDP & PII Compliance)

This repository demonstrates a production-ready PySpark Structured Streaming pipeline designed for Customer Data Platforms (CDP). It processes Kafka event streams through a Medallion Architecture (Bronze, Silver, Gold) on Azure Databricks.

## Architecture Highlights
* **Bronze Layer:** Ingests raw JSON payloads from Apache Kafka handling schema drift via `PERMISSIVE` mode.
* **Silver Layer:** Implements a robust Dead Letter Queue (DLQ) pattern to quarantine malformed records and a dynamic PII masking framework (SHA-256) for GDPR compliance.
* **Gold Layer:** Performs stateful aggregations using **Salting techniques** to mitigate data skew and Delta Lake `MERGE` operations for idempotent upserts.

## Engineering Practices
* **Object-Oriented Programming (OOP):** Abstract Base Classes (`ABC`) and the Template Method pattern are used to ensure DRY principles and modularity across pipeline stages.
* **Spark Optimization:** Adaptive Query Execution (AQE), Dynamic Partition Pruning, and `OPTIMIZE ZORDER BY` are configured for performance tuning.


# PySpark Medallion Streaming Pipeline (Event-Driven Architecture)

## Overview
Below is an overview of a recent end-to-end production pipeline I architected, coded, and supported. This solution leverages PySpark Structured Streaming and OOP principles to process event-driven data at scale.

## 1. Architecture & Context 
I implemented a Medallion Architecture (Bronze, Silver, Gold) on Azure Databricks to process real-time telemetry events from Kafka. To ensure maintainability and CI/CD, I designed an object-oriented framework using the `abc` Python module. The core execution logic was abstracted into a `MedallionStageBase` class using a template method pattern, allowing seamless polymorphic execution of each layer.

* **Bronze:** Ingested raw JSON payloads from Kafka into Delta Lake using `PERMISSIVE` modes to capture schema drifts.
* **Silver:** Data cleansing, deduplication, and a Dead Letter Queue (DLQ) pattern to quarantine malformed records without halting the stream.
* **Gold:** Performed complex stateful aggregations. I implemented salting techniques to mitigate data skew during Broadcast Joins and used Delta Lake `MERGE` operations for idempotent incremental upserts.

## 2. Data Scale
* **Volume:** Processed highly volatile event streams peaking at thousands of events per second.
* **Cluster Configuration:** Managed execution over optimized Databricks compute, tuning memory fractions and enabling Adaptive Query Execution (AQE) with dynamic partition coalescing.

## 3. Testing & Production Safeguards
* **Dead Letter Queue (Quarantine):** I designed the pipeline with runtime data quality gates. Using Spark's `PERMISSIVE` mode, malformed JSON payloads or schema mismatches are automatically detected and routed to a segregated Quarantine Delta table. This ensures the streaming micro-batch never crashes in production while preserving the corrupt payloads for debugging.
* **Schema Enforcement & Monitoring:** Leveraged Delta Lake's strict schema enforcement to prevent invalid data types from corrupting the Silver and Gold layers. I also implemented Spark Accumulators to track the volume of quarantined records, providing observability for production support.

## 4. Measurable Results
* **Performance:** Eliminated out-of-memory errors during skewed data aggregations by injecting random salts into the keys, resulting in a balanced execution plan.
* **Query Optimization:** Applied `OPTIMIZE ZORDER BY` on the Gold layer partitions, reducing data scanning times and associated compute costs for downstream BI consumers.
* **Code Maintainability:** Transitioning from monolithic PySpark scripts to the OOP framework reduced code duplication by 30% and accelerated the deployment of new data ingestion sources.

---
*Developed by Juan Alonso Zarate*