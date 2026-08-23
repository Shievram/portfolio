# Project 2: Amazon Product Analytics Batch ETL Pipeline

## Overview

This project builds a batch analytics pipeline using an Amazon product review CSV dataset. The dataset is uploaded to Cloud Storage, loaded into a BigQuery staging table, cleaned and transformed using SQL, modelled into dimension and fact tables, and visualized through Looker Studio dashboards.

## Architecture

```text
amazon.csv
   ↓
Cloud Storage Raw Layer
   ↓
BigQuery Staging Table
   ↓
BigQuery SQL Transformation
   ↓
BigQuery Warehouse Tables
   ↓
BigQuery Mart Views
   ↓
Looker Studio Dashboard
```

## Main Services

- Cloud Storage
- BigQuery
- SQL
- Looker Studio

No Python script is required for this project because the transformation is performed using BigQuery SQL.

## GCP Console Navigation

### Upload CSV to Cloud Storage

```text
Google Cloud Console → Cloud Storage → Buckets → raw_amazon → Upload files
```

### Create BigQuery Datasets and Tables

```text
Google Cloud Console → BigQuery → SQL workspace → New query
```

### Load CSV into BigQuery

```text
BigQuery → Dataset amazon_stg → Create table → Source: Google Cloud Storage
```

or use the `bq load` command shown below.

### Dashboard

```text
Looker Studio → Create → Report → Add data → BigQuery → amazon_mart views
```

## Dataset Description

The original CSV contains product, category, pricing, discount, rating, rating count, review and product-link fields.

Main fields:

```text
product_id, product_name, category, discounted_price, actual_price,
discount_percentage, rating, rating_count, about_product,
user_id, user_name, review_id, review_title, review_content,
img_link, product_link
```

## Create BigQuery Datasets

```sql
CREATE SCHEMA IF NOT EXISTS `project-a76ee6b5-c5cd-4392-86d.amazon_stg` OPTIONS(location = 'asia-southeast1');
CREATE SCHEMA IF NOT EXISTS `project-a76ee6b5-c5cd-4392-86d.amazon_dw` OPTIONS(location = 'asia-southeast1');
CREATE SCHEMA IF NOT EXISTS `project-a76ee6b5-c5cd-4392-86d.amazon_mart` OPTIONS(location = 'asia-southeast1');
```

## Create Staging Table

```sql
CREATE OR REPLACE TABLE `project-a76ee6b5-c5cd-4392-86d.amazon_stg.stg_amazon_products`
(
  product_id STRING,
  product_name STRING,
  category STRING,
  discounted_price STRING,
  actual_price STRING,
  discount_percentage STRING,
  rating STRING,
  rating_count STRING,
  about_product STRING,
  user_id STRING,
  user_name STRING,
  review_id STRING,
  review_title STRING,
  review_content STRING,
  img_link STRING,
  product_link STRING
);
```

## Load CSV from Cloud Storage

```bash
bq load \
  --source_format=CSV \
  --skip_leading_rows=1 \
  --field_delimiter="," \
  --quote='"' \
  --allow_quoted_newlines \
  --replace \
  project-a76ee6b5-c5cd-4392-86d:amazon_stg.stg_amazon_products \
  gs://raw_amazon/amazon/raw/amazon.csv \
  product_id:STRING,product_name:STRING,category:STRING,discounted_price:STRING,actual_price:STRING,discount_percentage:STRING,rating:STRING,rating_count:STRING,about_product:STRING,user_id:STRING,user_name:STRING,review_id:STRING,review_title:STRING,review_content:STRING,img_link:STRING,product_link:STRING
```

## SQL Transformation Layer

### Category Cleaning Function

```sql
CREATE OR REPLACE FUNCTION `project-a76ee6b5-c5cd-4392-86d.amazon_dw.clean_category_label`(input STRING)
RETURNS STRING
AS (
  CASE
    WHEN input IS NULL THEN NULL
    ELSE TRIM(
      REGEXP_REPLACE(
        REGEXP_REPLACE(
          REGEXP_REPLACE(
            REGEXP_REPLACE(input, r'&', ' & '),
            r'([A-Z]+)([A-Z][a-z])', r'\1 \2'
          ),
          r'([a-z0-9])([A-Z])', r'\1 \2'
        ),
        r'\s+', ' '
      )
    )
  END
);
```

### Cleaned Product Table

```sql
CREATE OR REPLACE TABLE `project-a76ee6b5-c5cd-4392-86d.amazon_dw.product_cleaned` AS
SELECT
  product_id,
  product_name,
  category AS full_category,
  `project-a76ee6b5-c5cd-4392-86d.amazon_dw.clean_category_label`(SPLIT(category, '|')[SAFE_OFFSET(0)]) AS main_category,
  `project-a76ee6b5-c5cd-4392-86d.amazon_dw.clean_category_label`(SPLIT(category, '|')[SAFE_OFFSET(1)]) AS sub_category_1,
  `project-a76ee6b5-c5cd-4392-86d.amazon_dw.clean_category_label`(SPLIT(category, '|')[SAFE_OFFSET(2)]) AS sub_category_2,
  `project-a76ee6b5-c5cd-4392-86d.amazon_dw.clean_category_label`(SPLIT(category, '|')[SAFE_OFFSET(3)]) AS sub_category_3,
  `project-a76ee6b5-c5cd-4392-86d.amazon_dw.clean_category_label`(SPLIT(category, '|')[SAFE_OFFSET(4)]) AS sub_category_4,
  `project-a76ee6b5-c5cd-4392-86d.amazon_dw.clean_category_label`(SPLIT(category, '|')[SAFE_OFFSET(5)]) AS sub_category_5,
  SAFE_CAST(REGEXP_REPLACE(discounted_price, r'[₹,]', '') AS NUMERIC) AS discounted_price,
  SAFE_CAST(REGEXP_REPLACE(actual_price, r'[₹,]', '') AS NUMERIC) AS actual_price,
  SAFE_CAST(REGEXP_REPLACE(actual_price, r'[₹,]', '') AS NUMERIC)
  - SAFE_CAST(REGEXP_REPLACE(discounted_price, r'[₹,]', '') AS NUMERIC) AS discount_amount,
  SAFE_CAST(REGEXP_REPLACE(discount_percentage, r'[%]', '') AS NUMERIC) AS discount_percentage,
  SAFE_CAST(rating AS FLOAT64) AS rating,
  SAFE_CAST(REGEXP_REPLACE(rating_count, r'[,]', '') AS INT64) AS rating_count,
  about_product, user_id, user_name, review_id, review_title, review_content, img_link, product_link,
  CURRENT_TIMESTAMP() AS load_timestamp
FROM `project-a76ee6b5-c5cd-4392-86d.amazon_stg.stg_amazon_products`
WHERE product_id IS NOT NULL;
```

### Product Dimension Table

```sql
CREATE OR REPLACE TABLE `project-a76ee6b5-c5cd-4392-86d.amazon_dw.dim_product` AS
WITH ranked AS (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY rating_count DESC NULLS LAST) AS rn
  FROM `project-a76ee6b5-c5cd-4392-86d.amazon_dw.product_cleaned`
)
SELECT
  product_id, product_name, full_category, main_category, sub_category_1,
  sub_category_2, sub_category_3, sub_category_4, sub_category_5,
  about_product, img_link, product_link, load_timestamp
FROM ranked
WHERE rn = 1;
```

### Product Metrics Fact Table

```sql
CREATE OR REPLACE TABLE `project-a76ee6b5-c5cd-4392-86d.amazon_dw.fact_product_metrics` AS
WITH ranked AS (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY rating_count DESC NULLS LAST) AS rn
  FROM `project-a76ee6b5-c5cd-4392-86d.amazon_dw.product_cleaned`
)
SELECT
  product_id, discounted_price, actual_price, actual_price - discounted_price AS discount_amount,
  discount_percentage, rating, rating_count,
  CASE
    WHEN rating >= 4.5 THEN 'Excellent'
    WHEN rating >= 4.0 THEN 'Good'
    WHEN rating >= 3.0 THEN 'Average'
    WHEN rating IS NULL THEN 'Unknown'
    ELSE 'Low'
  END AS rating_category,
  CASE
    WHEN discount_percentage >= 70 THEN 'Very High Discount'
    WHEN discount_percentage >= 40 THEN 'High Discount'
    WHEN discount_percentage >= 20 THEN 'Moderate Discount'
    WHEN discount_percentage IS NULL THEN 'Unknown'
    ELSE 'Low Discount'
  END AS discount_category,
  CURRENT_TIMESTAMP() AS load_timestamp
FROM ranked
WHERE rn = 1;
```

## Mart Views

### Category Performance View

```sql
CREATE OR REPLACE VIEW `project-a76ee6b5-c5cd-4392-86d.amazon_mart.vw_category_performance` AS
SELECT
  p.main_category,
  COUNT(DISTINCT p.product_id) AS total_products,
  AVG(m.rating) AS average_rating,
  SUM(m.rating_count) AS total_rating_count,
  AVG(m.discount_percentage) AS average_discount_percentage,
  AVG(m.actual_price) AS average_actual_price,
  AVG(m.discounted_price) AS average_discounted_price,
  AVG(m.discount_amount) AS average_discount_amount
FROM `project-a76ee6b5-c5cd-4392-86d.amazon_dw.dim_product` p
LEFT JOIN `project-a76ee6b5-c5cd-4392-86d.amazon_dw.fact_product_metrics` m
  ON p.product_id = m.product_id
GROUP BY p.main_category;
```

### Top Discounted Products

```sql
CREATE OR REPLACE VIEW `project-a76ee6b5-c5cd-4392-86d.amazon_mart.vw_top_discounted_products` AS
SELECT p.product_id, p.product_name, p.main_category, p.sub_category_1,
  m.actual_price, m.discounted_price, m.discount_amount, m.discount_percentage,
  m.rating, m.rating_count, m.discount_category
FROM `project-a76ee6b5-c5cd-4392-86d.amazon_dw.dim_product` p
LEFT JOIN `project-a76ee6b5-c5cd-4392-86d.amazon_dw.fact_product_metrics` m
  ON p.product_id = m.product_id
WHERE m.discount_percentage IS NOT NULL
ORDER BY m.discount_percentage DESC;
```

### Top Rated Products

```sql
CREATE OR REPLACE VIEW `project-a76ee6b5-c5cd-4392-86d.amazon_mart.vw_top_rated_products` AS
SELECT p.product_id, p.product_name, p.main_category, p.sub_category_1,
  m.rating, m.rating_count, m.actual_price, m.discounted_price,
  m.discount_percentage, m.rating_category
FROM `project-a76ee6b5-c5cd-4392-86d.amazon_dw.dim_product` p
LEFT JOIN `project-a76ee6b5-c5cd-4392-86d.amazon_dw.fact_product_metrics` m
  ON p.product_id = m.product_id
WHERE m.rating IS NOT NULL AND m.rating_count IS NOT NULL
ORDER BY m.rating DESC, m.rating_count DESC;
```

### Rating vs Discount View

```sql
CREATE OR REPLACE VIEW `project-a76ee6b5-c5cd-4392-86d.amazon_mart.vw_rating_vs_discount` AS
SELECT p.product_id, p.product_name, p.main_category, p.sub_category_1,
  m.rating, m.rating_count, m.discount_percentage, m.discount_category, m.rating_category
FROM `project-a76ee6b5-c5cd-4392-86d.amazon_dw.dim_product` p
LEFT JOIN `project-a76ee6b5-c5cd-4392-86d.amazon_dw.fact_product_metrics` m
  ON p.product_id = m.product_id
WHERE m.rating IS NOT NULL AND m.discount_percentage IS NOT NULL;
```

## Dashboard Pages

### Page 1: Product Overview

- Total products
- Average rating
- Average discount percentage
- Products by category
- Category performance table

### Page 2: Pricing and Discount Analysis

- Average actual price
- Average discounted price
- Top discounted products
- Discount by category
- Price vs discount chart

### Page 3: Rating and Review Analysis

- Average rating by category
- Top rated products
- Rating vs discount chart

### Page 4: Rating Volume Analysis

- Rating count by category
- Popular products by review/rating volume

## Portfolio Explanation

> This project demonstrates a batch data warehousing workflow using Cloud Storage and BigQuery. Raw Amazon product data is loaded into a staging table, transformed using SQL into cleaned warehouse tables, and modelled into business-ready mart views for category, pricing, discount, rating, and product-performance analysis.
