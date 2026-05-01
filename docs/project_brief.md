# Project Brief

## Problem
E-commerce companies generate massive amounts of transactional data across orders, customers, products, payments, and reviews. This data sits in operational systems and isn't structured for analytical queries. Without a proper data platform, business teams can't answer questions like:

- What's our customer lifetime value by region?
- Which product categories drive the most repeat purchases?
- How does delivery time affect review scores and revenue?
- What's our customer churn pattern by cohort?

## Solution
Build an end-to-end data platform that:
1. Ingests raw e-commerce transactional data
2. Stores it in a data lake layer (MinIO)
3. Loads it into an analytical warehouse (PostgreSQL)
4. Models it into a star schema using dbt
5. Validates quality with automated tests
6. Serves business questions through a BI dashboard
7. Adds real-time streaming for live order metrics

## Dataset
Olist Brazilian E-commerce Public Dataset (Kaggle) — 100k+ orders, 9 related tables, real-world data quality issues.

## Stack
PostgreSQL, MinIO, Apache Airflow, dbt, Great Expectations, Apache Kafka, Apache Spark, Metabase, Docker, GitHub Actions.

## Success criteria
- Pipeline runs end-to-end with `docker-compose up` and one Airflow DAG trigger
- Star schema supports all 7 business questions in the dashboard
- All dbt tests pass
- Streaming dashboard updates in real time