# Project 1A: BNM Exchange Rate Incremental ETL Pipeline

## Overview

This project builds an incremental exchange-rate ETL pipeline using Bank Negara Malaysia exchange-rate data. The pipeline extracts exchange-rate data from an API, stores raw JSON in Cloud Storage, transforms selected currencies, loads clean rows into BigQuery, and visualizes daily and historical currency movement in Looker Studio.

## Architecture

```text
BNM OpenAPI
   ↓
Python ETL Script
   ↓
Cloud Storage Raw JSON
   ↓
BigQuery Clean Table
   ↓
BigQuery Analysis Views
   ↓
Looker Studio Dashboard
```

Automation:

```text
Cloud Scheduler → Cloud Run Job → Python ETL → Cloud Storage + BigQuery
```

## Main Services

- Python
- Cloud Storage
- BigQuery
- Artifact Registry
- Cloud Build
- Cloud Run Job
- Cloud Scheduler
- Looker Studio

## GCP Console Navigation

### Create BigQuery Dataset and Tables

```text
Google Cloud Console → BigQuery → SQL workspace → New query
```

### Check Raw JSON Files

```text
Google Cloud Console → Cloud Storage → Buckets → select raw bucket
```

### Build and Check Container Image

```text
Google Cloud Console → Artifact Registry → Repositories → bnm-fx-repo
```

### Check Cloud Run Job

```text
Google Cloud Console → Cloud Run → Jobs → bnm-fx-etl-job
```

### Check Scheduler

```text
Google Cloud Console → Cloud Scheduler → bnm-fx scheduler job
```

### Dashboard

```text
Looker Studio → Report → Add data → BigQuery → fx_dataset views
```

## Selected Currencies

```text
USD, SGD, EUR, GBP, JPY, CNY, THB, IDR
```

## BigQuery Table

```sql
CREATE SCHEMA IF NOT EXISTS `project-a76ee6b5-c5cd-4392-86d.fx_dataset`;

CREATE TABLE IF NOT EXISTS `project-a76ee6b5-c5cd-4392-86d.fx_dataset.bnm_exchange_rates`
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
  load_timestamp TIMESTAMP
)
PARTITION BY rate_date
CLUSTER BY currency_code;
```

## Main Python ETL Script

File suggestion:

```text
project-1-bnm-fx-etl/src/main.py
```

```python
import json
import os
from datetime import datetime, timezone

import requests
from google.cloud import bigquery, storage

PROJECT_ID = os.getenv("PROJECT_ID", "project-a76ee6b5-c5cd-4392-86d")
BUCKET_NAME = os.getenv("BUCKET_NAME", "bnm-fx-raw")
DATASET_ID = os.getenv("DATASET_ID", "fx_dataset")
TABLE_ID = os.getenv("TABLE_ID", "bnm_exchange_rates")
BNM_SESSION = os.getenv("BNM_SESSION", "1200")
BNM_QUOTE = os.getenv("BNM_QUOTE", "rm")

SELECTED_CURRENCIES = ["USD", "SGD", "EUR", "GBP", "JPY", "CNY", "THB", "IDR"]

CURRENCY_METADATA = {
    "USD": {"currency_name": "US Dollar", "country": "United States", "region": "North America", "is_asean": False, "currency_category": "Major"},
    "SGD": {"currency_name": "Singapore Dollar", "country": "Singapore", "region": "ASEAN", "is_asean": True, "currency_category": "ASEAN"},
    "EUR": {"currency_name": "Euro", "country": "Euro Area", "region": "Europe", "is_asean": False, "currency_category": "Major"},
    "GBP": {"currency_name": "British Pound", "country": "United Kingdom", "region": "Europe", "is_asean": False, "currency_category": "Major"},
    "JPY": {"currency_name": "Japanese Yen", "country": "Japan", "region": "Asia", "is_asean": False, "currency_category": "Major"},
    "CNY": {"currency_name": "Chinese Yuan", "country": "China", "region": "Asia", "is_asean": False, "currency_category": "Regional"},
    "THB": {"currency_name": "Thai Baht", "country": "Thailand", "region": "ASEAN", "is_asean": True, "currency_category": "ASEAN"},
    "IDR": {"currency_name": "Indonesian Rupiah", "country": "Indonesia", "region": "ASEAN", "is_asean": True, "currency_category": "ASEAN"},
}


def safe_float(value):
    if value in [None, "", "-"]:
        return None
    try:
        return float(value)
    except Exception:
        return None


def extract_data():
    url = "https://api.bnm.gov.my/public/exchange-rate"
    headers = {"Accept": "application/vnd.BNM.API.v1+json"}
    params = {"session": BNM_SESSION, "quote": BNM_QUOTE}
    response = requests.get(url, headers=headers, params=params, timeout=30)
    response.raise_for_status()
    return response.json()


def save_raw_to_gcs(raw_data):
    storage_client = storage.Client(project=PROJECT_ID)
    bucket = storage_client.bucket(BUCKET_NAME)

    load_time = datetime.now(timezone.utc)
    object_name = f"bnm_exchange_rate/raw/year={load_time:%Y}/month={load_time:%m}/day={load_time:%d}/exchange_rate_{load_time:%Y%m%d_%H%M%S}.json"

    blob = bucket.blob(object_name)
    blob.upload_from_string(json.dumps(raw_data), content_type="application/json")

    return f"gs://{BUCKET_NAME}/{object_name}"


def transform_data(raw_data, raw_gcs_path):
    rows = []
    records = raw_data.get("data", [])

    for item in records:
        currency_code = item.get("currency_code") or item.get("currency")
        if currency_code not in SELECTED_CURRENCIES:
            continue

        metadata = CURRENCY_METADATA.get(currency_code, {})
        rate_date = item.get("rate_date") or item.get("date")

        rows.append({
            "rate_date": rate_date,
            "rate_session": BNM_SESSION,
            "quote_type": BNM_QUOTE,
            "currency_code": currency_code,
            "currency_name": metadata.get("currency_name"),
            "country": metadata.get("country"),
            "region": metadata.get("region"),
            "is_asean": metadata.get("is_asean"),
            "currency_category": metadata.get("currency_category"),
            "unit": safe_float(item.get("unit")) or 1,
            "buying_rate": safe_float(item.get("buying_rate")),
            "selling_rate": safe_float(item.get("selling_rate")),
            "middle_rate": safe_float(item.get("middle_rate")),
            "source_name": "BNM OpenAPI",
            "raw_gcs_path": raw_gcs_path,
            "load_timestamp": datetime.now(timezone.utc).isoformat(),
        })

    return rows


def load_incremental(rows):
    if not rows:
        print("No rows to load.")
        return

    client = bigquery.Client(project=PROJECT_ID)
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

    rate_dates = sorted({row["rate_date"] for row in rows if row.get("rate_date")})
    for rate_date in rate_dates:
        delete_query = f"""
        DELETE FROM `{table_ref}`
        WHERE rate_date = DATE('{rate_date}')
          AND rate_session = '{BNM_SESSION}'
          AND quote_type = '{BNM_QUOTE}'
        """
        client.query(delete_query).result()

    errors = client.insert_rows_json(table_ref, rows)
    if errors:
        raise RuntimeError(errors)

    print(f"Loaded {len(rows)} rows into {table_ref}")


def main():
    raw_data = extract_data()
    raw_gcs_path = save_raw_to_gcs(raw_data)
    rows = transform_data(raw_data, raw_gcs_path)
    load_incremental(rows)


if __name__ == "__main__":
    main()
```

## BigQuery Analysis Views

### Main Analysis View

```sql
CREATE OR REPLACE VIEW `project-a76ee6b5-c5cd-4392-86d.fx_dataset.vw_bnm_exchange_rate_analysis` AS
WITH deduped AS (
  SELECT
    rate_date, rate_session, quote_type, currency_code, currency_name, country,
    region, is_asean, currency_category, unit, buying_rate, selling_rate,
    middle_rate, source_name, raw_gcs_path, load_timestamp
  FROM `project-a76ee6b5-c5cd-4392-86d.fx_dataset.bnm_exchange_rates`
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
  source_name, raw_gcs_path, load_timestamp
FROM base;
```

### Latest Currency Movement View

```sql
CREATE OR REPLACE VIEW `project-a76ee6b5-c5cd-4392-86d.fx_dataset.vw_bnm_latest_currency_movement` AS
SELECT
  rate_date, currency_code, currency_name, country, region, is_asean,
  currency_category, middle_rate AS latest_rate, middle_rate,
  previous_middle_rate, daily_change, pct_change, myr_direction,
  movement_severity, is_market_closed, load_timestamp
FROM `project-a76ee6b5-c5cd-4392-86d.fx_dataset.vw_bnm_exchange_rate_analysis`
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY currency_code
  ORDER BY rate_date DESC, load_timestamp DESC
) = 1;
```

### Historical Chart View

```sql
CREATE OR REPLACE VIEW `project-a76ee6b5-c5cd-4392-86d.fx_dataset.vw_bnm_historical_chart` AS
SELECT rate_date, currency_code, currency_name, country, region, currency_category,
  middle_rate, previous_middle_rate, daily_change, pct_change, myr_direction, movement_severity
FROM `project-a76ee6b5-c5cd-4392-86d.fx_dataset.vw_bnm_exchange_rate_analysis`
WHERE middle_rate > 0 AND is_market_closed = FALSE;
```

## Cloud Run and Scheduler Commands

```bash
gcloud builds submit \
  --tag asia-southeast1-docker.pkg.dev/project-a76ee6b5-c5cd-4392-86d/bnm-fx-repo/bnm-fx-etl:latest

gcloud run jobs create bnm-fx-etl-job \
  --image asia-southeast1-docker.pkg.dev/project-a76ee6b5-c5cd-4392-86d/bnm-fx-repo/bnm-fx-etl:latest \
  --region asia-southeast1 \
  --memory 512Mi \
  --cpu 1 \
  --max-retries 1 \
  --task-timeout 30m

gcloud scheduler jobs create http bnm-fx-daily-scheduler \
  --location asia-southeast1 \
  --schedule "0 18 * * *" \
  --time-zone "Asia/Kuala_Lumpur" \
  --uri "https://run.googleapis.com/v2/projects/project-a76ee6b5-c5cd-4392-86d/locations/asia-southeast1/jobs/bnm-fx-etl-job:run" \
  --http-method POST \
  --oauth-token-scope "https://www.googleapis.com/auth/cloud-platform"
```

## Dashboard Pages

### Page 1: Daily Exchange Rate Analysis

- Latest USD, SGD, EUR, GBP rates
- Daily percentage change
- Latest movement table
- MYR direction and movement severity

### Page 2: Historical Trend Analysis

- Historical middle-rate trend
- Historical percentage change
- Currency filter
- Major currency trend table

## Portfolio Explanation

> This project demonstrates an automated API-based ETL pipeline on Google Cloud. Exchange-rate data is extracted using Python, stored as raw JSON in Cloud Storage, transformed into a clean BigQuery table, deduplicated using SQL views, and visualized in Looker Studio. Cloud Run Jobs and Cloud Scheduler automate the daily execution of the pipeline.
