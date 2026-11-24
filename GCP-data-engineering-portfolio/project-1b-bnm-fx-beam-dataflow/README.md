# Project 1B: BNM Exchange Rate Apache Beam / Dataflow Pipeline

## Overview

This project extends [Project 1A: BNM Exchange Rate Incremental ETL Pipeline](../project-1-bnm-fx-etl/README.md) using Apache Beam and Cloud Dataflow. Both projects use the BNM Open API and the same JSON response structure. This pipeline archives the raw JSON in Cloud Storage, validates and enriches selected currency records, writes valid and rejected records separately to BigQuery, and supports analysis and pipeline monitoring in Looker Studio.

## Architecture

```text
BNM Open API (JSON)
        ↓
Cloud Storage (raw JSON)
        ↓
Python preparation + Apache Beam / Cloud Dataflow
        ↓
  ┌─────┴─────┐
  ↓           ↓
BigQuery    BigQuery
Clean Table Error Table
  └─────┬─────┘
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

## Source and JSON Contract

The source uses the same request configuration and field mapping documented in [Project 1A's Python ETL script](../project-1-bnm-fx-etl/README.md#main-python-etl-script).

- Endpoint: `https://api.bnm.gov.my/public/exchange-rate`
- Accept header: `application/vnd.BNM.API.v1+json`
- Query parameters: `session=1200` and `quote=rm` by default
- Expected response: a JSON object containing a `data` array of currency records

The following illustrative payload follows the flat field structure in Project 1A's documented parser. These are sample values, not live exchange rates.

```json
{
  "data": [
    {
      "currency_code": "USD",
      "rate_date": "2026-07-24",
      "unit": 1,
      "buying_rate": 4.10,
      "selling_rate": 4.14,
      "middle_rate": 4.12
    }
  ]
}
```

The parser accepts `currency_code` or `currency`, and `rate_date` or `date`, as in Project 1A. Currency codes are normalized to uppercase. This project retains its original four selected currencies (USD, SGD, EUR and GBP) and metadata; Project 1A's additional currency metadata can be added if needed.

Optional start and end dates filter the records returned by the API. The endpoint used in Project 1A is not assumed to provide a historical date range. Historical backfills require archived JSON snapshots or a separately verified historical API endpoint.

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
import json
import uuid
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation

import apache_beam as beam
import requests
from apache_beam.io import WriteToBigQuery
from apache_beam.options.pipeline_options import PipelineOptions
from google.cloud import storage

CURRENCY_METADATA = {
    "USD": {"currency_name": "US Dollar", "country": "United States", "region": "North America", "is_asean": False, "currency_category": "Major"},
    "SGD": {"currency_name": "Singapore Dollar", "country": "Singapore", "region": "ASEAN", "is_asean": True, "currency_category": "ASEAN"},
    "EUR": {"currency_name": "Euro", "country": "Euro Area", "region": "Europe", "is_asean": False, "currency_category": "Major"},
    "GBP": {"currency_name": "British Pound", "country": "United Kingdom", "region": "Europe", "is_asean": False, "currency_category": "Major"},
}

SELECTED_CURRENCIES = ["USD", "SGD", "EUR", "GBP"]


def decimal_string(value):
    if value in [None, "", "-"]:
        return None
    try:
        number = Decimal(str(value))
        if not number.is_finite():
            return None
        return format(number.quantize(Decimal("0.000001")), "f")
    except (InvalidOperation, ValueError, TypeError):
        return None


def prepare_source_data(start_date, end_date, bucket_name, project_id,
                        session="1200", quote="rm"):
    source_url = "https://api.bnm.gov.my/public/exchange-rate"
    headers = {"Accept": "application/vnd.BNM.API.v1+json"}
    params = {"session": session, "quote": quote}
    response = requests.get(source_url, headers=headers, params=params, timeout=30)
    response.raise_for_status()
    raw_data = response.json()

    job_run_id = str(uuid.uuid4())
    load_timestamp = datetime.now(timezone.utc).isoformat()
    pipeline_name = f"bnm_fx_beam_daily{session}"
    rate_items = []
    errors = []

    storage_client = storage.Client(project=project_id)
    bucket = storage_client.bucket(bucket_name)
    raw_path = (
        f"raw/session={session}/quote={quote}/"
        f"job_run_id={job_run_id}/exchange_rate.json"
    )
    bucket.blob(raw_path).upload_from_string(
        json.dumps(raw_data), content_type="application/json"
    )
    raw_gcs_path = f"gs://{bucket_name}/{raw_path}"

    if not isinstance(raw_data, dict) or not isinstance(raw_data.get("data"), list):
        raise ValueError(f"Expected a JSON data array; raw snapshot: {raw_gcs_path}")

    def reject(item, currency_code, rate_date, error_type, message):
        errors.append({
            "error_timestamp": load_timestamp,
            "rate_date": rate_date,
            "currency_code": currency_code,
            "error_type": error_type,
            "error_message": message,
            "raw_response": json.dumps(item),
            "pipeline_name": pipeline_name,
            "job_run_id": job_run_id,
        })

    for item in raw_data["data"]:
        if not isinstance(item, dict):
            reject(item, None, None, "INVALID_RECORD", "Expected a currency object")
            continue

        code = str(item.get("currency_code") or item.get("currency") or "").strip().upper()
        if not code:
            reject(item, None, None, "MISSING_CURRENCY", "Currency code is missing")
            continue
        if code not in SELECTED_CURRENCIES:
            continue

        # Match the field names and aliases used in Project 1A.
        try:
            rate_date = date.fromisoformat(
                item.get("rate_date") or item.get("date") or ""
            ).isoformat()
        except (ValueError, TypeError):
            reject(item, code, None, "INVALID_DATE", "Expected a valid ISO rate date")
            continue

        if (start_date and rate_date < start_date) or (end_date and rate_date > end_date):
            continue

        middle = decimal_string(item.get("middle_rate"))
        buying = decimal_string(item.get("buying_rate"))
        selling = decimal_string(item.get("selling_rate"))
        unit_value = item.get("unit")
        unit = "1" if unit_value in [None, "", "-"] else decimal_string(unit_value)

        if middle is None or Decimal(middle) <= 0:
            reject(item, code, rate_date, "INVALID_RATE_VALUE", "Missing or invalid middle rate")
            continue
        if unit is None or Decimal(unit) <= 0:
            reject(item, code, rate_date, "INVALID_UNIT", "Unit must be positive")
            continue
        if any(
            item.get(field) not in [None, "", "-"] and
            (value is None or Decimal(value) <= 0)
            for field, value in [("buying_rate", buying), ("selling_rate", selling)]
        ):
            reject(item, code, rate_date, "INVALID_RATE_VALUE", "Invalid buying or selling rate")
            continue

        meta = CURRENCY_METADATA[code]
        rate_items.append({
            "rate_date": rate_date,
            "rate_session": session,
            "quote_type": quote,
            "currency_code": code,
            "currency_name": meta["currency_name"],
            "country": meta["country"],
            "region": meta["region"],
            "is_asean": meta["is_asean"],
            "currency_category": meta["currency_category"],
            "unit": unit,
            "buying_rate": buying,
            "selling_rate": selling,
            "middle_rate": middle,
            "source_name": "BNM OpenAPI",
            "raw_gcs_path": raw_gcs_path,
            "pipeline_name": pipeline_name,
            "job_run_id": job_run_id,
            "load_timestamp": load_timestamp,
        })

    print(f"Run {job_run_id}: {len(rate_items)} valid rows, {len(errors)} rejected rows; raw snapshot: {raw_gcs_path}")
    return rate_items, errors


def run():
    parser = argparse.ArgumentParser()
    parser.add_argument("--gcp_project_id", required=True)
    parser.add_argument("--bucket_name", required=True)
    parser.add_argument("--session", default="1200")
    parser.add_argument("--quote", default="rm")
    parser.add_argument("--start_date", type=lambda value: date.fromisoformat(value).isoformat())
    parser.add_argument("--end_date", type=lambda value: date.fromisoformat(value).isoformat())
    known_args, pipeline_args = parser.parse_known_args()
    if known_args.start_date and known_args.end_date and known_args.start_date > known_args.end_date:
        parser.error("--start_date must not be later than --end_date")

    rate_items, initial_errors = prepare_source_data(
        known_args.start_date,
        known_args.end_date,
        known_args.bucket_name,
        known_args.gcp_project_id,
        known_args.session,
        known_args.quote,
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
  --session 1200 \
  --quote rm
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
  --session 1200 \
  --quote rm
```

## Engineering Decisions and Operational Behavior

| Concern | Design |
|---|---|
| Source consistency | Reuses Project 1A's BNM Open API JSON field mapping, session and quote parameters. |
| Raw data and lineage | Preserves the full parsed response in a run-specific Cloud Storage JSON object; valid rows retain the object URI and run ID. |
| Incremental operation | Each invocation processes the returned exchange-rate snapshot, optionally restricted to a date range. It does not download a full historical CSV. |
| Validation | Checks dates, positive middle rates and units, and supplied buying/selling rates before loading; rejected selected records go to the error table. Missing optional buying/selling rates remain null. |
| Beam execution | For this small API snapshot, extraction, landing and validation run in Python before Beam starts; Beam handles the prepared success/error writes. Larger backfills could move transformation into Beam workers. |
| Duplicate handling | Writes append-only to the clean table. The analytical view selects the latest row by rate date, session, quote and currency. This removes duplicates from reporting, not from physical storage. |
| Failure recovery | HTTP, JSON decoding and Cloud Storage failures stop preparation. An invalid response envelope fails after archiving when possible. Row-level rejections enter the error table; BigQuery write failures require runner/log inspection. |
| Monitoring | The job-run ID connects success and error counts. A run with no returned matching records produces no rows; check runner logs as well as monitoring views. |

For scheduled execution, a Cloud Run Job or other orchestrator can invoke the pipeline with the daily session/quote parameters. A Cloud Scheduler trigger at 18:00 Asia/Kuala_Lumpur, as in Project 1A, is a possible daily schedule. This README describes the invocation; it does not establish that a scheduler is currently deployed.

The SQL below preserves the existing analysis and monitoring views. Monitoring counts reflect the rows written to the two tables, not independent confirmation that the full Dataflow job succeeded.

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

> This project extends Project 1A's BNM Open API JSON ingestion with Apache Beam and Cloud Dataflow. The pipeline archives raw JSON in Cloud Storage, prepares and validates currency records, writes valid and rejected rows separately to BigQuery, deduplicates analytical results using the business key, and supports Looker Studio reporting and run-level monitoring.
