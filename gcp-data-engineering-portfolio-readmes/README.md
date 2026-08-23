# Google Cloud Data Engineering Portfolio

This repository contains four Google Cloud data engineering projects built around batch ETL, API ingestion, Apache Beam/Dataflow, BigQuery data warehousing, and real-time streaming analytics.

## Projects

| No. | Project | Main Focus | Main Services |
|---|---|---|---|
| 1A | BNM Exchange Rate Incremental ETL Pipeline | API-based daily ETL | Cloud Run Job, Cloud Scheduler, Cloud Storage, BigQuery, Looker Studio |
| 1B | BNM Exchange Rate Apache Beam / Dataflow Pipeline | Scalable batch processing | Apache Beam, Dataflow, Cloud Storage, BigQuery, Looker Studio |
| 2 | Amazon Product Analytics Batch Pipeline | CSV batch warehouse and dashboard | Cloud Storage, BigQuery, SQL, Looker Studio |
| 3 | Real-Time Retail Event Streaming Pipeline | Streaming ingestion and transformation | Cloud Run Job, Pub/Sub, Dataflow, BigQuery, Looker Studio |

## Project ID and Region

```text
Project ID: project-a76ee6b5-c5cd-4392-86d
Region: asia-southeast1
```

If you reuse these projects under another Google Cloud project, replace the project ID in all commands and SQL queries.

## Portfolio Summary

These projects demonstrate:

- Python-based extraction and event generation
- Cloud Storage raw data landing
- BigQuery schema design, partitioning and clustering
- Incremental loading and backfilling
- Apache Beam and Dataflow processing
- Pub/Sub real-time streaming
- Cloud Run Jobs and Cloud Scheduler automation
- SQL transformation views and business marts
- Looker Studio dashboards
- Pipeline monitoring and error-handling concepts

## Repository Structure

```text
gcp-data-engineering-portfolio/
├── README.md
├── project-1-bnm-fx-etl/
│   └── README.md
├── project-1b-bnm-fx-beam-dataflow/
│   └── README.md
├── project-2-amazon-product-analytics/
│   └── README.md
└── project-3-retail-realtime-pipeline/
    └── README.md
```

## General Google Cloud Navigation

### BigQuery

```text
Google Cloud Console → BigQuery → SQL workspace → New query
```

Use this for creating datasets, tables, views, and validation queries.

### Cloud Storage

```text
Google Cloud Console → Cloud Storage → Buckets
```

Use this to verify uploaded raw files, staging files, or Dataflow temporary files.

### Cloud Run Jobs

```text
Google Cloud Console → Cloud Run → Jobs
```

Use this to check Cloud Run Job configuration, executions, logs, environment variables, and service account.

### Pub/Sub

```text
Google Cloud Console → Pub/Sub → Topics / Subscriptions
```

Use this to check event topics, BigQuery subscriptions, and Dataflow subscriptions.

### Dataflow

```text
Google Cloud Console → Dataflow → Jobs
```

Use this to monitor running/failed Dataflow jobs, job graph, worker logs, and throughput.

### Looker Studio

```text
Looker Studio → Create → Report → Add data → BigQuery
```

Use BigQuery views as dashboard data sources instead of raw tables where possible.

## References

- Cloud Run Jobs: https://cloud.google.com/run/docs/create-jobs
- Cloud Run scheduled jobs: https://cloud.google.com/run/docs/execute/jobs-on-schedule
- Pub/Sub BigQuery subscriptions: https://docs.cloud.google.com/pubsub/docs/create-bigquery-subscription
- Pub/Sub to BigQuery with Dataflow: https://docs.cloud.google.com/dataflow/docs/tutorials/dataflow-stream-to-bigquery
- BigQuery loading data from Cloud Storage: https://cloud.google.com/bigquery/docs/loading-data-cloud-storage-csv
