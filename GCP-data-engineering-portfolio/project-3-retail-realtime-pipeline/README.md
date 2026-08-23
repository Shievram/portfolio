# Project 3: Real-Time Retail Event Streaming Pipeline using Cloud Run, Pub/Sub, Dataflow and BigQuery

## Overview

This project implements a real-time retail event streaming pipeline on Google Cloud. A Cloud Run Job generates synthetic customer journey events and publishes them to Pub/Sub. Pub/Sub fans out the same events into a raw BigQuery table and a Dataflow streaming transformation pipeline. Dataflow validates, cleans, enriches, and transforms the events before loading them into a curated BigQuery table. SQL views support real-time dashboard reporting in Looker Studio.

## Architecture

```text
Cloud Scheduler
      ↓
Cloud Run Job
Retail Event Producer
      ↓
Pub/Sub Topic
retail-order-events
      ↓
 ┌─────────────────────────────────┐
 │                                 │
 ↓                                 ↓
BigQuery Subscription           Dataflow Streaming Pipeline
Raw BigQuery Table              Validation + Transformation
retail_events_raw                    ↓
                                Clean BigQuery Table
                                retail_events_cleaned
                                      ↓
                                SQL Analytical Views
                                      ↓
                                Looker Studio Dashboard
```

## Main Services

- Cloud Run Job
- Cloud Scheduler
- Pub/Sub
- Dataflow
- Apache Beam
- BigQuery
- Cloud Storage
- Artifact Registry
- Cloud Build
- Looker Studio

## GCP Console Navigation

### Cloud Run Job

```text
Google Cloud Console → Cloud Run → Jobs → retail-event-producer-job
```

Use this to check producer job executions and logs.

### Pub/Sub

```text
Google Cloud Console → Pub/Sub → Topics → retail-order-events
Google Cloud Console → Pub/Sub → Subscriptions
```

Use this to verify:

```text
retail-order-events-raw-bq-sub
retail-order-events-dataflow-sub
```

### Dataflow

```text
Google Cloud Console → Dataflow → Jobs → retail-streaming-transform
```

Use this to check the streaming transformation job graph, worker logs, and errors.

### BigQuery

```text
Google Cloud Console → BigQuery → retail_streaming dataset
```

Use this to check:

```text
retail_events_raw
retail_events_cleaned
retail_events_errors
SQL views
```

### Looker Studio

```text
Looker Studio → Add data → BigQuery → retail_streaming views
```

Use transformed views, not the raw table, for dashboard charts.

## Phase 1: Setup

```bash
PROJECT_ID="project-a76ee6b5-c5cd-4392-86d"
REGION="asia-southeast1"

gcloud config set project $PROJECT_ID
gcloud config set run/region $REGION

gcloud services enable \
  run.googleapis.com \
  pubsub.googleapis.com \
  bigquery.googleapis.com \
  bigquerystorage.googleapis.com \
  dataflow.googleapis.com \
  cloudbuild.googleapis.com \
  artifactregistry.googleapis.com \
  storage.googleapis.com

cd ~
mkdir -p project-3-retail-realtime-pipeline/producer
mkdir -p project-3-retail-realtime-pipeline/dataflow
mkdir -p project-3-retail-realtime-pipeline/sql
mkdir -p project-3-retail-realtime-pipeline/docs
mkdir -p project-3-retail-realtime-pipeline/dashboard/screenshots
cd project-3-retail-realtime-pipeline
```

## Phase 2: BigQuery Tables

```sql
CREATE SCHEMA IF NOT EXISTS `project-a76ee6b5-c5cd-4392-86d.retail_streaming`
OPTIONS(location = 'asia-southeast1');

CREATE OR REPLACE TABLE `project-a76ee6b5-c5cd-4392-86d.retail_streaming.retail_events_raw`
(
  event_id STRING,
  event_timestamp TIMESTAMP,
  customer_id STRING,
  session_id STRING,
  event_sequence_number INT64,
  event_type STRING,
  order_id STRING,
  product_id STRING,
  product_name STRING,
  category STRING,
  quantity INT64,
  unit_price NUMERIC,
  discount_amount NUMERIC,
  shipping_fee NUMERIC,
  total_amount NUMERIC,
  payment_method STRING,
  payment_status STRING,
  order_status STRING,
  country STRING,
  city STRING,
  device_type STRING,
  traffic_source STRING,
  campaign_id STRING,
  ingestion_source STRING,
  environment STRING
)
PARTITION BY DATE(event_timestamp)
CLUSTER BY event_type, category, traffic_source;

CREATE OR REPLACE TABLE `project-a76ee6b5-c5cd-4392-86d.retail_streaming.retail_events_cleaned`
(
  event_id STRING,
  event_timestamp TIMESTAMP,
  event_date DATE,
  event_hour INT64,
  customer_id STRING,
  session_id STRING,
  event_sequence_number INT64,
  event_type STRING,
  funnel_step INT64,
  order_id STRING,
  product_id STRING,
  product_name STRING,
  category STRING,
  quantity INT64,
  unit_price NUMERIC,
  discount_amount NUMERIC,
  shipping_fee NUMERIC,
  total_amount NUMERIC,
  revenue_amount NUMERIC,
  payment_method STRING,
  payment_status STRING,
  order_status STRING,
  is_purchase BOOL,
  is_successful_purchase BOOL,
  country STRING,
  city STRING,
  device_type STRING,
  traffic_source STRING,
  campaign_id STRING,
  is_campaign_traffic BOOL,
  ingestion_source STRING,
  environment STRING,
  processing_timestamp TIMESTAMP
)
PARTITION BY event_date
CLUSTER BY event_type, category, traffic_source;

CREATE OR REPLACE TABLE `project-a76ee6b5-c5cd-4392-86d.retail_streaming.retail_events_errors`
(
  error_timestamp TIMESTAMP,
  error_type STRING,
  error_message STRING,
  raw_message STRING,
  processing_stage STRING
);
```

## Phase 3: Pub/Sub Resources

```bash
gcloud pubsub topics create retail-order-events

gcloud pubsub subscriptions create retail-order-events-dataflow-sub \
  --topic=retail-order-events

PROJECT_NUMBER=$(gcloud projects describe $PROJECT_ID --format="value(projectNumber)")
PUBSUB_SA="service-${PROJECT_NUMBER}@gcp-sa-pubsub.iam.gserviceaccount.com"

gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:$PUBSUB_SA" \
  --role="roles/bigquery.dataEditor"

gcloud pubsub subscriptions create retail-order-events-raw-bq-sub \
  --topic=retail-order-events \
  --bigquery-table=project-a76ee6b5-c5cd-4392-86d.retail_streaming.retail_events_raw \
  --use-table-schema
```

## Phase 4: Cloud Storage and IAM

```bash
BUCKET_NAME="retail-streaming-dataflow-${PROJECT_NUMBER}"

gcloud storage buckets create gs://$BUCKET_NAME \
  --location=$REGION || true

gcloud iam service-accounts create retail-producer-sa \
  --display-name="Retail Event Producer Service Account" || true

gcloud pubsub topics add-iam-policy-binding retail-order-events \
  --member="serviceAccount:retail-producer-sa@project-a76ee6b5-c5cd-4392-86d.iam.gserviceaccount.com" \
  --role="roles/pubsub.publisher"

WORKER_SA="${PROJECT_NUMBER}-compute@developer.gserviceaccount.com"

gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:$WORKER_SA" \
  --role="roles/dataflow.worker"

gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:$WORKER_SA" \
  --role="roles/pubsub.subscriber"

gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:$WORKER_SA" \
  --role="roles/bigquery.dataEditor"

gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:$WORKER_SA" \
  --role="roles/bigquery.jobUser"

gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:$WORKER_SA" \
  --role="roles/logging.logWriter"

gcloud storage buckets add-iam-policy-binding gs://$BUCKET_NAME \
  --member="serviceAccount:$WORKER_SA" \
  --role="roles/storage.objectAdmin"
```

## Phase 5: Cloud Run Producer

### producer/requirements.txt

```text
google-cloud-pubsub
```

### producer/publish_retail_events.py

```python
import json
import os
import random
import time
import uuid
from datetime import datetime, timezone

from google.cloud import pubsub_v1

PROJECT_ID = os.getenv("PROJECT_ID", "project-a76ee6b5-c5cd-4392-86d")
TOPIC_ID = os.getenv("TOPIC_ID", "retail-order-events")
SESSIONS = int(os.getenv("SESSIONS", "100"))
SLEEP_SECONDS = float(os.getenv("SLEEP_SECONDS", "0.2"))
ENVIRONMENT = os.getenv("ENVIRONMENT", "demo")
SEED = int(os.getenv("SEED", "42"))

PRODUCTS = [
    {"product_id": "P001", "product_name": "Gaming Laptop", "category": "Electronics", "unit_price": 4299.00},
    {"product_id": "P002", "product_name": "Wireless Mouse", "category": "Accessories", "unit_price": 89.00},
    {"product_id": "P003", "product_name": "Mechanical Keyboard", "category": "Accessories", "unit_price": 239.00},
    {"product_id": "P004", "product_name": "USB-C Hub", "category": "Accessories", "unit_price": 129.00},
    {"product_id": "P005", "product_name": "Noise Cancelling Headphones", "category": "Audio", "unit_price": 599.00},
    {"product_id": "P006", "product_name": "Smart Watch", "category": "Wearables", "unit_price": 399.00},
    {"product_id": "P007", "product_name": "Portable Monitor", "category": "Electronics", "unit_price": 799.00},
    {"product_id": "P008", "product_name": "Laptop Stand", "category": "Accessories", "unit_price": 79.00},
]

DEVICE_TYPES = ["Desktop", "Mobile", "Tablet"]
TRAFFIC_SOURCES = ["Google", "Direct", "Facebook", "Instagram", "Email"]
PAYMENT_METHODS = ["Card", "Online Banking", "E-Wallet", "Cash on Delivery"]
CITIES = ["Kuala Lumpur", "Shah Alam", "Petaling Jaya", "Johor Bahru", "Penang", "Kuantan"]


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def money(value):
    return f"{float(value):.2f}"


def create_session_events():
    customer_id = f"C{random.randint(1000, 9999)}"
    session_id = f"S{uuid.uuid4()}"
    product = random.choice(PRODUCTS)
    device_type = random.choice(DEVICE_TYPES)
    traffic_source = random.choice(TRAFFIC_SOURCES)
    city = random.choice(CITIES)

    campaign_id = None
    if traffic_source in ["Google", "Facebook", "Instagram", "Email"]:
        campaign_id = f"CAMPAIGN_{traffic_source.upper()}_{random.randint(1, 5):03d}"

    journey = ["page_view", "product_view"]

    if random.random() < 0.70:
        journey.append("add_to_cart")
    if "add_to_cart" in journey and random.random() < 0.55:
        journey.append("checkout")
    if "checkout" in journey and random.random() < 0.65:
        journey.append("purchase")

    events = []

    for sequence_number, event_type in enumerate(journey, start=1):
        quantity = 0
        discount_amount = 0.00
        shipping_fee = 0.00
        total_amount = 0.00
        payment_method = None
        payment_status = None
        order_status = None
        order_id = None

        if event_type in ["add_to_cart", "checkout", "purchase"]:
            quantity = random.randint(1, 3)
            subtotal = product["unit_price"] * quantity
            discount_amount = round(subtotal * random.choice([0.00, 0.05, 0.10, 0.15]), 2)
            shipping_fee = random.choice([0.00, 5.00, 10.00])
            total_amount = round(subtotal - discount_amount + shipping_fee, 2)

        if event_type in ["checkout", "purchase"]:
            order_id = f"ORD{random.randint(100000, 999999)}"

        if event_type == "purchase":
            payment_method = random.choice(PAYMENT_METHODS)
            payment_status = random.choices(["Paid", "Failed"], weights=[90, 10], k=1)[0]
            order_status = "Completed" if payment_status == "Paid" else "Payment Failed"

        event = {
            "event_id": f"evt_{uuid.uuid4()}",
            "event_timestamp": utc_now(),
            "customer_id": customer_id,
            "session_id": session_id,
            "event_sequence_number": sequence_number,
            "event_type": event_type,
            "order_id": order_id,
            "product_id": product["product_id"],
            "product_name": product["product_name"],
            "category": product["category"],
            "quantity": quantity,
            "unit_price": money(product["unit_price"]),
            "discount_amount": money(discount_amount),
            "shipping_fee": money(shipping_fee),
            "total_amount": money(total_amount),
            "payment_method": payment_method,
            "payment_status": payment_status,
            "order_status": order_status,
            "country": "MY",
            "city": city,
            "device_type": device_type,
            "traffic_source": traffic_source,
            "campaign_id": campaign_id,
            "ingestion_source": "cloud_run_job",
            "environment": ENVIRONMENT,
        }
        events.append(event)

    return events


def publish_event(publisher, topic_path, event):
    message = json.dumps(event).encode("utf-8")
    future = publisher.publish(topic_path, message)
    message_id = future.result()
    print(f"Published {message_id} | {event['event_type']} | {event['product_name']} | RM {event['total_amount']}")


def main():
    random.seed(SEED)
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

    for _ in range(SESSIONS):
        for event in create_session_events():
            publish_event(publisher, topic_path, event)
            time.sleep(SLEEP_SECONDS)


if __name__ == "__main__":
    main()
```

### producer/Dockerfile

```dockerfile
FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY publish_retail_events.py .

CMD ["python", "publish_retail_events.py"]
```

### Build and Create Cloud Run Job

```bash
gcloud artifacts repositories create retail-streaming-repo \
  --repository-format=docker \
  --location=asia-southeast1 \
  --description="Retail streaming pipeline container images" || true

gcloud builds submit producer \
  --tag asia-southeast1-docker.pkg.dev/project-a76ee6b5-c5cd-4392-86d/retail-streaming-repo/retail-event-producer:latest

gcloud run jobs create retail-event-producer-job \
  --image asia-southeast1-docker.pkg.dev/project-a76ee6b5-c5cd-4392-86d/retail-streaming-repo/retail-event-producer:latest \
  --region asia-southeast1 \
  --service-account retail-producer-sa@project-a76ee6b5-c5cd-4392-86d.iam.gserviceaccount.com \
  --memory 512Mi \
  --cpu 1 \
  --max-retries 1 \
  --task-timeout 30m \
  --set-env-vars PROJECT_ID=project-a76ee6b5-c5cd-4392-86d,TOPIC_ID=retail-order-events,SESSIONS=100,SLEEP_SECONDS=0.2,ENVIRONMENT=demo,SEED=42
```

## Phase 6: Dataflow Streaming Transformation

Dataflow performs the main transformation step:

```text
Parse JSON
Validate required fields
Standardize text
Create event_date and event_hour
Create funnel_step
Create purchase flags
Calculate revenue_amount
Add processing_timestamp
Write valid rows to retail_events_cleaned
Write invalid rows to retail_events_errors
```

### dataflow/requirements.txt

```text
apache-beam[gcp]==2.75.0
```

### dataflow/retail_streaming_pipeline.py

```python
import json
from datetime import datetime, timezone

import apache_beam as beam
from apache_beam import pvalue
from apache_beam.io import ReadFromPubSub, WriteToBigQuery
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions


class RetailPipelineOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument("--input_subscription", required=True)
        parser.add_argument("--output_table", required=True)
        parser.add_argument("--error_table", required=True)


def to_money_string(value):
    if value is None:
        return "0.00"
    try:
        return f"{float(value):.2f}"
    except Exception:
        return "0.00"


def clean_text(value, default=None):
    if value is None:
        return default
    value = str(value).strip()
    if value == "":
        return default
    return value


class TransformRetailEvent(beam.DoFn):
    ERROR_TAG = "errors"

    def process(self, message):
        raw_message = message.decode("utf-8")

        try:
            event = json.loads(raw_message)
            required_fields = [
                "event_id", "event_timestamp", "customer_id", "session_id",
                "event_sequence_number", "event_type", "product_id",
                "product_name", "category"
            ]

            missing_fields = [
                field for field in required_fields
                if field not in event or event[field] is None or str(event[field]).strip() == ""
            ]
            if missing_fields:
                raise ValueError(f"Missing required fields: {missing_fields}")

            event_type = clean_text(event.get("event_type"), "").lower()
            funnel_steps = {
                "page_view": 1,
                "product_view": 2,
                "add_to_cart": 3,
                "checkout": 4,
                "purchase": 5,
            }
            if event_type not in funnel_steps:
                raise ValueError(f"Invalid event_type: {event_type}")

            event_timestamp = clean_text(event.get("event_timestamp"))
            event_date = event_timestamp[:10]
            event_hour = int(event_timestamp[11:13])
            payment_status = clean_text(event.get("payment_status"))
            total_amount = float(event.get("total_amount") or 0)
            is_purchase = event_type == "purchase"
            is_successful_purchase = event_type == "purchase" and payment_status == "Paid"
            revenue_amount = total_amount if is_successful_purchase else 0

            cleaned = {
                "event_id": clean_text(event.get("event_id")),
                "event_timestamp": event_timestamp,
                "event_date": event_date,
                "event_hour": event_hour,
                "customer_id": clean_text(event.get("customer_id")),
                "session_id": clean_text(event.get("session_id")),
                "event_sequence_number": int(event.get("event_sequence_number")),
                "event_type": event_type,
                "funnel_step": funnel_steps[event_type],
                "order_id": clean_text(event.get("order_id")),
                "product_id": clean_text(event.get("product_id")),
                "product_name": clean_text(event.get("product_name")).title(),
                "category": clean_text(event.get("category")).title(),
                "quantity": int(event.get("quantity") or 0),
                "unit_price": to_money_string(event.get("unit_price")),
                "discount_amount": to_money_string(event.get("discount_amount")),
                "shipping_fee": to_money_string(event.get("shipping_fee")),
                "total_amount": to_money_string(event.get("total_amount")),
                "revenue_amount": to_money_string(revenue_amount),
                "payment_method": clean_text(event.get("payment_method")),
                "payment_status": payment_status,
                "order_status": clean_text(event.get("order_status")),
                "is_purchase": is_purchase,
                "is_successful_purchase": is_successful_purchase,
                "country": clean_text(event.get("country"), "Unknown"),
                "city": clean_text(event.get("city"), "Unknown").title(),
                "device_type": clean_text(event.get("device_type"), "Unknown").title(),
                "traffic_source": clean_text(event.get("traffic_source"), "Unknown").title(),
                "campaign_id": clean_text(event.get("campaign_id")),
                "is_campaign_traffic": event.get("campaign_id") is not None,
                "ingestion_source": clean_text(event.get("ingestion_source"), "unknown"),
                "environment": clean_text(event.get("environment"), "unknown"),
                "processing_timestamp": datetime.now(timezone.utc).isoformat(),
            }
            yield cleaned

        except Exception as error:
            yield pvalue.TaggedOutput(self.ERROR_TAG, {
                "error_timestamp": datetime.now(timezone.utc).isoformat(),
                "error_type": type(error).__name__,
                "error_message": str(error),
                "raw_message": raw_message,
                "processing_stage": "dataflow_transformation",
            })


def run():
    pipeline_options = PipelineOptions()
    retail_options = pipeline_options.view_as(RetailPipelineOptions)
    pipeline_options.view_as(StandardOptions).streaming = True

    cleaned_schema = (
        "event_id:STRING,event_timestamp:TIMESTAMP,event_date:DATE,event_hour:INT64,"
        "customer_id:STRING,session_id:STRING,event_sequence_number:INT64,"
        "event_type:STRING,funnel_step:INT64,order_id:STRING,"
        "product_id:STRING,product_name:STRING,category:STRING,"
        "quantity:INT64,unit_price:NUMERIC,discount_amount:NUMERIC,shipping_fee:NUMERIC,"
        "total_amount:NUMERIC,revenue_amount:NUMERIC,"
        "payment_method:STRING,payment_status:STRING,order_status:STRING,"
        "is_purchase:BOOL,is_successful_purchase:BOOL,"
        "country:STRING,city:STRING,device_type:STRING,traffic_source:STRING,"
        "campaign_id:STRING,is_campaign_traffic:BOOL,"
        "ingestion_source:STRING,environment:STRING,processing_timestamp:TIMESTAMP"
    )

    error_schema = "error_timestamp:TIMESTAMP,error_type:STRING,error_message:STRING,raw_message:STRING,processing_stage:STRING"

    with beam.Pipeline(options=pipeline_options) as pipeline:
        transformed = (
            pipeline
            | "Read from PubSub" >> ReadFromPubSub(subscription=retail_options.input_subscription)
            | "Transform retail events" >> beam.ParDo(TransformRetailEvent()).with_outputs(
                TransformRetailEvent.ERROR_TAG,
                main="cleaned",
            )
        )

        transformed.cleaned | "Write cleaned events" >> WriteToBigQuery(
            table=retail_options.output_table,
            schema=cleaned_schema,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
            method=WriteToBigQuery.Method.STREAMING_INSERTS,
        )

        transformed.errors | "Write errors" >> WriteToBigQuery(
            table=retail_options.error_table,
            schema=error_schema,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
            method=WriteToBigQuery.Method.STREAMING_INSERTS,
        )


if __name__ == "__main__":
    run()
```

### Start Dataflow

```bash
cd ~/project-3-retail-realtime-pipeline
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r dataflow/requirements.txt

python dataflow/retail_streaming_pipeline.py \
  --runner DataflowRunner \
  --project project-a76ee6b5-c5cd-4392-86d \
  --region asia-southeast1 \
  --job_name retail-streaming-transform \
  --temp_location gs://$BUCKET_NAME/temp \
  --staging_location gs://$BUCKET_NAME/staging \
  --requirements_file dataflow/requirements.txt \
  --save_main_session \
  --input_subscription projects/project-a76ee6b5-c5cd-4392-86d/subscriptions/retail-order-events-dataflow-sub \
  --output_table project-a76ee6b5-c5cd-4392-86d:retail_streaming.retail_events_cleaned \
  --error_table project-a76ee6b5-c5cd-4392-86d:retail_streaming.retail_events_errors
```

## Phase 7: Execute the Producer

Run this only after Dataflow is running:

```bash
gcloud run jobs execute retail-event-producer-job \
  --region asia-southeast1 \
  --wait
```

## Phase 8: SQL Analytical Views

### Real-Time Sales Summary

```sql
CREATE OR REPLACE VIEW `project-a76ee6b5-c5cd-4392-86d.retail_streaming.vw_realtime_sales_summary` AS
SELECT
  COUNT(*) AS total_events,
  COUNT(DISTINCT customer_id) AS unique_customers,
  COUNT(DISTINCT session_id) AS total_sessions,
  COUNTIF(is_successful_purchase) AS total_successful_purchases,
  SUM(revenue_amount) AS total_revenue,
  AVG(CASE WHEN is_successful_purchase THEN revenue_amount END) AS average_order_value,
  MAX(event_timestamp) AS latest_event_timestamp
FROM `project-a76ee6b5-c5cd-4392-86d.retail_streaming.retail_events_cleaned`;
```

### Events Over Time

```sql
CREATE OR REPLACE VIEW `project-a76ee6b5-c5cd-4392-86d.retail_streaming.vw_events_over_time` AS
SELECT
  TIMESTAMP_TRUNC(event_timestamp, MINUTE) AS event_minute,
  event_type,
  COUNT(*) AS total_events
FROM `project-a76ee6b5-c5cd-4392-86d.retail_streaming.retail_events_cleaned`
GROUP BY event_minute, event_type;
```

### Conversion Funnel

```sql
CREATE OR REPLACE VIEW `project-a76ee6b5-c5cd-4392-86d.retail_streaming.vw_conversion_funnel` AS
WITH funnel AS (
  SELECT 'page_view' AS event_type, 1 AS funnel_step UNION ALL
  SELECT 'product_view', 2 UNION ALL
  SELECT 'add_to_cart', 3 UNION ALL
  SELECT 'checkout', 4 UNION ALL
  SELECT 'purchase', 5
),
event_counts AS (
  SELECT
    event_type,
    COUNT(*) AS total_events,
    COUNT(DISTINCT session_id) AS sessions
  FROM `project-a76ee6b5-c5cd-4392-86d.retail_streaming.retail_events_cleaned`
  GROUP BY event_type
),
base AS (
  SELECT
    f.funnel_step,
    f.event_type,
    COALESCE(e.total_events, 0) AS total_events,
    COALESCE(e.sessions, 0) AS sessions
  FROM funnel f
  LEFT JOIN event_counts e ON f.event_type = e.event_type
)
SELECT
  funnel_step,
  event_type,
  total_events,
  sessions,
  SAFE_DIVIDE(sessions, FIRST_VALUE(sessions) OVER (ORDER BY funnel_step)) * 100 AS conversion_rate_from_page_view,
  SAFE_DIVIDE(sessions, LAG(sessions) OVER (ORDER BY funnel_step)) * 100 AS stage_to_stage_conversion_rate,
  100 - SAFE_DIVIDE(sessions, LAG(sessions) OVER (ORDER BY funnel_step)) * 100 AS stage_dropoff_rate
FROM base
ORDER BY funnel_step;
```

### Product Performance

```sql
CREATE OR REPLACE VIEW `project-a76ee6b5-c5cd-4392-86d.retail_streaming.vw_product_performance` AS
SELECT
  product_id,
  product_name,
  category,
  COUNT(*) AS total_events,
  COUNTIF(event_type = 'product_view') AS product_views,
  COUNTIF(event_type = 'add_to_cart') AS add_to_cart_events,
  COUNTIF(event_type = 'checkout') AS checkout_events,
  COUNTIF(event_type = 'purchase') AS purchase_events,
  COUNTIF(is_successful_purchase) AS successful_purchases,
  SUM(revenue_amount) AS total_revenue,
  SAFE_DIVIDE(COUNTIF(is_successful_purchase), COUNTIF(event_type = 'product_view')) * 100 AS product_view_to_purchase_rate
FROM `project-a76ee6b5-c5cd-4392-86d.retail_streaming.retail_events_cleaned`
GROUP BY product_id, product_name, category;
```

### Traffic Source Performance

```sql
CREATE OR REPLACE VIEW `project-a76ee6b5-c5cd-4392-86d.retail_streaming.vw_traffic_source_performance` AS
SELECT
  traffic_source,
  COUNT(*) AS total_events,
  COUNT(DISTINCT customer_id) AS unique_customers,
  COUNT(DISTINCT session_id) AS total_sessions,
  COUNTIF(is_successful_purchase) AS successful_purchases,
  SUM(revenue_amount) AS total_revenue,
  SAFE_DIVIDE(COUNTIF(is_successful_purchase), COUNT(DISTINCT session_id)) * 100 AS session_purchase_conversion_rate
FROM `project-a76ee6b5-c5cd-4392-86d.retail_streaming.retail_events_cleaned`
GROUP BY traffic_source;
```

### Device Performance

```sql
CREATE OR REPLACE VIEW `project-a76ee6b5-c5cd-4392-86d.retail_streaming.vw_device_performance` AS
SELECT
  device_type,
  COUNT(*) AS total_events,
  COUNT(DISTINCT customer_id) AS unique_customers,
  COUNT(DISTINCT session_id) AS total_sessions,
  COUNTIF(is_successful_purchase) AS successful_purchases,
  SUM(revenue_amount) AS total_revenue
FROM `project-a76ee6b5-c5cd-4392-86d.retail_streaming.retail_events_cleaned`
GROUP BY device_type;
```

### Session Summary

```sql
CREATE OR REPLACE VIEW `project-a76ee6b5-c5cd-4392-86d.retail_streaming.vw_session_summary` AS
SELECT
  session_id,
  customer_id,
  MIN(event_timestamp) AS session_start_time,
  MAX(event_timestamp) AS session_end_time,
  TIMESTAMP_DIFF(MAX(event_timestamp), MIN(event_timestamp), SECOND) AS session_duration_seconds,
  COUNT(*) AS total_events,
  MAX(IF(event_type = 'page_view', 1, 0)) AS has_page_view,
  MAX(IF(event_type = 'product_view', 1, 0)) AS has_product_view,
  MAX(IF(event_type = 'add_to_cart', 1, 0)) AS has_add_to_cart,
  MAX(IF(event_type = 'checkout', 1, 0)) AS has_checkout,
  MAX(IF(event_type = 'purchase', 1, 0)) AS has_purchase,
  MAX(IF(is_successful_purchase, 1, 0)) AS has_successful_purchase
FROM `project-a76ee6b5-c5cd-4392-86d.retail_streaming.retail_events_cleaned`
GROUP BY session_id, customer_id;
```

### Pipeline Health Check

```sql
CREATE OR REPLACE VIEW `project-a76ee6b5-c5cd-4392-86d.retail_streaming.vw_pipeline_health_check` AS
SELECT
  COUNT(*) AS total_clean_events,
  COUNT(DISTINCT event_id) AS unique_events,
  COUNT(*) - COUNT(DISTINCT event_id) AS duplicate_events,
  COUNTIF(event_id IS NULL) AS missing_event_id,
  COUNTIF(event_timestamp IS NULL) AS missing_event_timestamp,
  COUNTIF(session_id IS NULL) AS missing_session_id,
  COUNTIF(event_type IS NULL) AS missing_event_type,
  MAX(event_timestamp) AS latest_event_timestamp,
  TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MAX(event_timestamp), MINUTE) AS minutes_since_latest_event
FROM `project-a76ee6b5-c5cd-4392-86d.retail_streaming.retail_events_cleaned`;
```

### Error Monitoring

```sql
CREATE OR REPLACE VIEW `project-a76ee6b5-c5cd-4392-86d.retail_streaming.vw_error_monitoring` AS
SELECT
  error_type,
  processing_stage,
  COUNT(*) AS total_errors,
  MAX(error_timestamp) AS latest_error_timestamp
FROM `project-a76ee6b5-c5cd-4392-86d.retail_streaming.retail_events_errors`
GROUP BY error_type, processing_stage;
```

## Phase 9: Dashboard Pages

### Page 1: Real-Time Retail Overview

- Total events
- Unique customers
- Total sessions
- Total revenue
- Latest event timestamp
- Events over time
- Events by type

### Page 2: Conversion Funnel

- Sessions with add to cart
- Sessions with purchase
- Sessions by funnel stage
- Conversion rate from page view
- Stage-to-stage conversion rate
- Drop-off rate

### Page 3: Product Performance

- Total successful purchases
- Revenue by product
- Product views by product
- Product performance table

### Page 4: Traffic and Device Analysis

- Revenue by traffic source
- Conversion rate by traffic source
- Events by device type
- Device performance table

### Page 5: Pipeline Monitoring

- Total clean events
- Duplicate events
- Missing event IDs
- Minutes since latest event
- Error monitoring table

## Optional: Schedule the Producer

```bash
SERVICE_ACCOUNT="retail-producer-sa@project-a76ee6b5-c5cd-4392-86d.iam.gserviceaccount.com"

gcloud run jobs add-iam-policy-binding retail-event-producer-job \
  --region asia-southeast1 \
  --member="serviceAccount:$SERVICE_ACCOUNT" \
  --role="roles/run.invoker"

gcloud scheduler jobs create http retail-event-producer-scheduler \
  --location asia-southeast1 \
  --schedule "*/15 * * * *" \
  --time-zone "Asia/Kuala_Lumpur" \
  --uri "https://run.googleapis.com/v2/projects/project-a76ee6b5-c5cd-4392-86d/locations/asia-southeast1/jobs/retail-event-producer-job:run" \
  --http-method POST \
  --oauth-service-account-email $SERVICE_ACCOUNT \
  --oauth-token-scope "https://www.googleapis.com/auth/cloud-platform"
```

## Stop Dataflow After Demo

```bash
gcloud dataflow jobs list --region asia-southeast1

gcloud dataflow jobs cancel YOUR_JOB_ID --region asia-southeast1
```

If the Cloud Scheduler job is active, pause it when not testing:

```bash
gcloud scheduler jobs pause retail-event-producer-scheduler --location asia-southeast1
```

## Portfolio Explanation

> This project demonstrates a real-time streaming analytics architecture on Google Cloud. Cloud Run Job generates customer journey events, Pub/Sub ingests the event stream, BigQuery subscription stores raw events, Dataflow performs real-time validation and transformation, and BigQuery SQL views power Looker Studio dashboards for sales, funnel conversion, product engagement, traffic source performance, device analysis, and pipeline health monitoring.
