# Project 1B: BNM Exchange Rate Apache Beam / Dataflow Pipeline

## Overview

This project extends the BNM exchange-rate use case using Apache Beam and Cloud Dataflow. The goal is to demonstrate a scalable processing design with raw data landing, BigQuery loading, validation, error handling, monitoring views, and a dashboard.

## Architecture

```text
data.gov.my daily_1200 exchange-rate CSV
        ↓
Apache Beam Pipeline
        ↓
Cloud Dataflow
        ↓
Cloud Storage Raw Files
        ↓
BigQuery Clean Table
        ↓
BigQuery Error Table
        ↓
BigQuery Analysis and Monitoring Views
        ↓
Looker Studio Dashboard
```

## Main Services

- Python
- Apache Beam
- Cloud Dataflow
- Cloud Storage
- BigQuery
- Looker Studio

## GCP Console Navigation

### Enable and Monitor Dataflow

```text
Google Cloud Console → Dataflow → Jobs → Region: asia-southeast1
```

### Check Raw Files

```text
Google Cloud Console → Cloud Storage → Buckets → bnm_fx_beam
```

### Check BigQuery Tables

```text
Google Cloud Console → BigQuery → fx_dataset → bnm_exchange_rates_beam / bnm_exchange_rate_errors
```

### Dashboard

```text
Looker Studio → Add data → BigQuery → fx_dataset views ending with _beam
```

## BigQuery Tables

```sql
CREATE SCHEMA IF NOT EXISTS `project-a76ee6b5-c5cd-4392-86d.fx_dataset`
OPTIONS(location = 'asia-southeast1');

CREATE TABLE IF NOT EXISTS `project-a76ee6b5-c5cd-4392-86d.fx_dataset.bnm_exchange_rates_beam`
(
  rate_date DATE,
  rate_session STRING,
  quote_type STRING,
  currency_code STRING,
  currency_name STRING,
  country STRING,
  region STRING,
  is_asean BOOL,
  currency_category STRING,
  unit NUMERIC,
  buying_rate NUMERIC,
  selling_rate NUMERIC,
  middle_rate NUMERIC,
  source_name STRING,
  raw_gcs_path STRING,
  pipeline_name STRING,
  job_run_id STRING,
  load_timestamp TIMESTAMP
)
PARTITION BY rate_date
CLUSTER BY currency_code;

CREATE TABLE IF NOT EXISTS `project-a76ee6b5-c5cd-4392-86d.fx_dataset.bnm_exchange_rate_errors`
(
  error_timestamp TIMESTAMP,
  rate_date DATE,
  currency_code STRING,
  error_type STRING,
  error_message STRING,
  raw_response STRING,
  pipeline_name STRING,
  job_run_id STRING
);
```

## Beam Pipeline Core Python

File suggestion:

```text
project-1b-bnm-fx-beam-dataflow/beam_pipeline/bnm_fx_beam_pipeline.py
```

```python
import argparse
import csv
import io
import json
import uuid
from datetime import datetime, timezone

import apache_beam as beam
import requests
from apache_beam.io import WriteToBigQuery
from apache_beam.options.pipeline_options import PipelineOptions
from google.cloud import storage

CURRENCY_METADATA = {
    "usd": {"currency_name": "US Dollar", "country": "United States", "region": "North America", "is_asean": False, "currency_category": "Major"},
    "sgd": {"currency_name": "Singapore Dollar", "country": "Singapore", "region": "ASEAN", "is_asean": True, "currency_category": "ASEAN"},
    "eur": {"currency_name": "Euro", "country": "Euro Area", "region": "Europe", "is_asean": False, "currency_category": "Major"},
    "gbp": {"currency_name": "British Pound", "country": "United Kingdom", "region": "Europe", "is_asean": False, "currency_category": "Major"},
}

SELECTED_CURRENCIES = ["usd", "sgd", "eur", "gbp"]


def decimal_string(value):
    if value in [None, "", "-"]:
        return None
    try:
        return f"{float(value):.6f}"
    except Exception:
        return None


def prepare_source_data(start_date, end_date, bucket_name, project_id):
    source_url = "https://storage.data.gov.my/finsector/exr/daily_1200.csv"
    response = requests.get(source_url, timeout=60)
    response.raise_for_status()

    rows = list(csv.DictReader(io.StringIO(response.text)))
    rate_items = []
    errors = []
    job_run_id = str(uuid.uuid4())
    load_timestamp = datetime.now(timezone.utc).isoformat()

    storage_client = storage.Client(project=project_id)
    bucket = storage_client.bucket(bucket_name)
    raw_path = f"raw/daily_1200/job_run_id={job_run_id}/daily_1200.csv"
    bucket.blob(raw_path).upload_from_string(response.text, content_type="text/csv")
    raw_gcs_path = f"gs://{bucket_name}/{raw_path}"

    for row in rows:
        rate_date = row.get("date")
        if not rate_date or not (start_date <= rate_date <= end_date):
            continue

        for code in SELECTED_CURRENCIES:
            rate = decimal_string(row.get(code))
            if rate is None:
                errors.append({
                    "error_timestamp": load_timestamp,
                    "rate_date": rate_date,
                    "currency_code": code.upper(),
                    "error_type": "INVALID_RATE_VALUE",
                    "error_message": f"Missing or invalid rate for {code}",
                    "raw_response": json.dumps(row),
                    "pipeline_name": "bnm_fx_beam_daily1200",
                    "job_run_id": job_run_id,
                })
                continue

            meta = CURRENCY_METADATA[code]
            rate_items.append({
                "rate_date": rate_date,
                "rate_session": "1200",
                "quote_type": "rm",
                "currency_code": code.upper(),
                "currency_name": meta["currency_name"],
                "country": meta["country"],
                "region": meta["region"],
                "is_asean": meta["is_asean"],
                "currency_category": meta["currency_category"],
                "unit": "1",
                "buying_rate": None,
                "selling_rate": None,
                "middle_rate": rate,
                "source_name": "data.gov.my daily_1200",
                "raw_gcs_path": raw_gcs_path,
                "pipeline_name": "bnm_fx_beam_daily1200",
                "job_run_id": job_run_id,
                "load_timestamp": load_timestamp,
            })

    return rate_items, errors


def run():
    parser = argparse.ArgumentParser()
    parser.add_argument("--gcp_project_id", required=True)
    parser.add_argument("--bucket_name", required=True)
    parser.add_argument("--start_date", required=True)
    parser.add_argument("--end_date", required=True)
    known_args, pipeline_args = parser.parse_known_args()

    rate_items, initial_errors = prepare_source_data(
        known_args.start_date,
        known_args.end_date,
        known_args.bucket_name,
        known_args.gcp_project_id,
    )

    pipeline_options = PipelineOptions(pipeline_args)

    with beam.Pipeline(options=pipeline_options) as pipeline:
        (
            pipeline
            | "Create exchange-rate rows" >> beam.Create(rate_items)
            | "Write success rows" >> WriteToBigQuery(
                table=f"{known_args.gcp_project_id}:fx_dataset.bnm_exchange_rates_beam",
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
                method=WriteToBigQuery.Method.STREAMING_INSERTS,
            )
        )

        (
            pipeline
            | "Create error rows" >> beam.Create(initial_errors)
            | "Write errors" >> WriteToBigQuery(
                table=f"{known_args.gcp_project_id}:fx_dataset.bnm_exchange_rate_errors",
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
                method=WriteToBigQuery.Method.STREAMING_INSERTS,
            )
        )


if __name__ == "__main__":
    run()
```

## Run Locally with DirectRunner

```bash
cd ~/project-1-bnm-fx-beam

python beam_pipeline/bnm_fx_beam_pipeline.py \
  --runner DirectRunner \
  --gcp_project_id project-a76ee6b5-c5cd-4392-86d \
  --bucket_name bnm_fx_beam \
  --start_date 2026-07-01 \
  --end_date 2026-07-24
```

## Run with DataflowRunner

```bash
cd ~/project-1-bnm-fx-beam

python beam_pipeline/bnm_fx_beam_pipeline.py \
  --runner DataflowRunner \
  --project project-a76ee6b5-c5cd-4392-86d \
  --region asia-southeast1 \
  --job_name bnm-fx-beam-daily1200-run \
  --temp_location gs://bnm_fx_beam/dataflow/temp \
  --staging_location gs://bnm_fx_beam/dataflow/staging \
  --requirements_file requirements.txt \
  --save_main_session \
  --gcp_project_id project-a76ee6b5-c5cd-4392-86d \
  --bucket_name bnm_fx_beam \
  --start_date 2026-07-01 \
  --end_date 2026-08-17
```

## Analysis Views

### Main Beam Analysis View

```sql
CREATE OR REPLACE VIEW `project-a76ee6b5-c5cd-4392-86d.fx_dataset.vw_bnm_exchange_rate_analysis_beam` AS
WITH deduped AS (
  SELECT
    rate_date, rate_session, quote_type, currency_code, currency_name, country,
    region, is_asean, currency_category, unit, buying_rate, selling_rate,
    middle_rate, source_name, raw_gcs_path, pipeline_name, job_run_id, load_timestamp
  FROM `project-a76ee6b5-c5cd-4392-86d.fx_dataset.bnm_exchange_rates_beam`
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY rate_date, rate_session, quote_type, currency_code
    ORDER BY load_timestamp DESC
  ) = 1
),
base AS (
  SELECT *,
    LAG(middle_rate) OVER (
      PARTITION BY currency_code, rate_session, quote_type
      ORDER BY rate_date
    ) AS previous_middle_rate
  FROM deduped
)
SELECT
  rate_date, rate_session, quote_type, currency_code, currency_name, country,
  region, is_asean, currency_category, unit, buying_rate, selling_rate,
  middle_rate, previous_middle_rate,
  CASE WHEN EXTRACT(DAYOFWEEK FROM rate_date) IN (1, 7) THEN 0
       ELSE middle_rate - previous_middle_rate END AS daily_change,
  CASE WHEN EXTRACT(DAYOFWEEK FROM rate_date) IN (1, 7) THEN 0
       ELSE SAFE_DIVIDE(middle_rate - previous_middle_rate, previous_middle_rate) * 100 END AS pct_change,
  CASE
    WHEN EXTRACT(DAYOFWEEK FROM rate_date) IN (1, 7) THEN 'Market Closed'
    WHEN previous_middle_rate IS NULL THEN 'No Previous Rate'
    WHEN middle_rate - previous_middle_rate > 0 THEN 'MYR Weakened'
    WHEN middle_rate - previous_middle_rate < 0 THEN 'MYR Strengthened'
    ELSE 'No Change'
  END AS myr_direction,
  CASE
    WHEN EXTRACT(DAYOFWEEK FROM rate_date) IN (1, 7) THEN 'Market Closed'
    WHEN previous_middle_rate IS NULL THEN 'No Previous Rate'
    WHEN ABS(SAFE_DIVIDE(middle_rate - previous_middle_rate, previous_middle_rate) * 100) >= 0.50 THEN 'High Movement'
    WHEN ABS(SAFE_DIVIDE(middle_rate - previous_middle_rate, previous_middle_rate) * 100) >= 0.20 THEN 'Moderate Movement'
    ELSE 'Low Movement'
  END AS movement_severity,
  CASE WHEN EXTRACT(DAYOFWEEK FROM rate_date) IN (1, 7) THEN TRUE ELSE FALSE END AS is_market_closed,
  source_name, raw_gcs_path, pipeline_name, job_run_id, load_timestamp
FROM base;
```

### Pipeline Monitoring View

```sql
CREATE OR REPLACE VIEW `project-a76ee6b5-c5cd-4392-86d.fx_dataset.vw_bnm_pipeline_monitoring_beam` AS
WITH success_summary AS (
  SELECT
    job_run_id, pipeline_name, COUNT(*) AS total_success_records,
    COUNT(DISTINCT currency_code) AS total_currencies,
    MIN(rate_date) AS min_rate_date,
    MAX(rate_date) AS max_rate_date,
    MAX(load_timestamp) AS latest_load_timestamp
  FROM `project-a76ee6b5-c5cd-4392-86d.fx_dataset.bnm_exchange_rates_beam`
  GROUP BY job_run_id, pipeline_name
),
error_summary AS (
  SELECT
    job_run_id, ANY_VALUE(pipeline_name) AS pipeline_name,
    COUNT(*) AS total_error_records,
    MAX(error_timestamp) AS latest_error_timestamp
  FROM `project-a76ee6b5-c5cd-4392-86d.fx_dataset.bnm_exchange_rate_errors`
  GROUP BY job_run_id
),
all_job_ids AS (
  SELECT job_run_id FROM success_summary
  UNION DISTINCT
  SELECT job_run_id FROM error_summary
)
SELECT
  j.job_run_id,
  COALESCE(s.pipeline_name, e.pipeline_name) AS pipeline_name,
  COALESCE(s.total_success_records, 0) AS total_success_records,
  COALESCE(e.total_error_records, 0) AS total_error_records,
  COALESCE(s.total_currencies, 0) AS total_currencies,
  s.min_rate_date,
  s.max_rate_date,
  CONCAT(CAST(s.min_rate_date AS STRING), ' to ', CAST(s.max_rate_date AS STRING)) AS load_period,
  COALESCE(s.latest_load_timestamp, e.latest_error_timestamp) AS latest_run_timestamp,
  CASE
    WHEN COALESCE(s.total_success_records, 0) > 0 AND COALESCE(e.total_error_records, 0) = 0 THEN 'Success'
    WHEN COALESCE(s.total_success_records, 0) > 0 AND COALESCE(e.total_error_records, 0) > 0 THEN 'Success With Errors'
    WHEN COALESCE(s.total_success_records, 0) = 0 AND COALESCE(e.total_error_records, 0) > 0 THEN 'No Data / Warning'
    ELSE 'Unknown'
  END AS pipeline_status
FROM all_job_ids j
LEFT JOIN success_summary s ON j.job_run_id = s.job_run_id
LEFT JOIN error_summary e ON j.job_run_id = e.job_run_id
ORDER BY latest_run_timestamp DESC;
```

## Dashboard Pages

### Page 1: Exchange Rate Overview

- Latest exchange rates
- Currency movement direction
- Movement severity

### Page 2: Historical Currency Trends

- Time-series middle-rate chart
- Currency filter
- Daily percentage movement

### Page 3: Pipeline Monitoring

- Total success records
- Total error records
- Job status table
- Error type summary

## Portfolio Explanation

> This project demonstrates how Apache Beam and Cloud Dataflow can be used to process exchange-rate data at scale. The pipeline lands raw source data in Cloud Storage, validates and writes processed rows to BigQuery, captures invalid records in an error table, and supports dashboard-level monitoring through SQL views.
