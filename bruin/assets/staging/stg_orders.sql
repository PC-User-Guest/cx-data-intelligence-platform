/* @bruin

name: staging.stg_orders
type: bq.sql
materialization:
  type: table
depends:
    - raw_zone.raw_orders

@bruin */

-- Cleans and deduplicates order events and computes delay indicators.
WITH source_data AS (
    SELECT
        order_id,
        customer_id,
        order_status,
        order_purchase_timestamp,
        order_approved_at,
        order_delivered_carrier_date,
        order_delivered_customer_date,
        order_estimated_delivery_date,
        price,
        freight_value,
        is_delayed_order,
        _dlt_load_id,
        _dlt_id
    FROM `raw_zone.raw_orders`
),
casted AS (
    SELECT
        CAST(order_id AS STRING) AS order_id,
        CAST(customer_id AS STRING) AS customer_id,
        COALESCE(NULLIF(CAST(order_status AS STRING), ''), 'unknown') AS order_status,
        SAFE_CAST(order_purchase_timestamp AS TIMESTAMP) AS order_purchase_timestamp,
        SAFE_CAST(order_approved_at AS TIMESTAMP) AS order_approved_at,
        SAFE_CAST(order_delivered_carrier_date AS TIMESTAMP) AS order_delivered_carrier_date,
        SAFE_CAST(order_delivered_customer_date AS TIMESTAMP) AS order_delivered_customer_date,
        SAFE_CAST(order_estimated_delivery_date AS TIMESTAMP) AS order_estimated_delivery_date,
        SAFE_CAST(price AS FLOAT64) AS price,
        SAFE_CAST(freight_value AS FLOAT64) AS freight_value,
        SAFE_CAST(is_delayed_order AS BOOL) AS is_delayed_order,
        _dlt_load_id,
        _dlt_id
    FROM source_data
),
deduplicated AS (
    SELECT
        * EXCEPT(row_num)
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY order_id
                ORDER BY _dlt_load_id DESC, order_purchase_timestamp DESC, _dlt_id DESC
            ) AS row_num
        FROM casted
        WHERE order_purchase_timestamp IS NOT NULL
    )
    WHERE row_num = 1
)
SELECT
    order_id,
    customer_id,
    order_status,
    order_purchase_timestamp,
    order_approved_at,
    order_delivered_carrier_date,
    order_delivered_customer_date,
    order_estimated_delivery_date,
    COALESCE(price, 0.0) AS price,
    COALESCE(freight_value, 0.0) AS freight_value,
    COALESCE(price, 0.0) + COALESCE(freight_value, 0.0) AS order_total_amount,
    COALESCE(
        is_delayed_order,
        order_delivered_customer_date > order_estimated_delivery_date
    ) AS is_delayed_order,
    CASE
        WHEN order_delivered_customer_date IS NULL OR order_estimated_delivery_date IS NULL THEN NULL
        ELSE DATE_DIFF(
            DATE(order_delivered_customer_date),
            DATE(order_estimated_delivery_date),
            DAY
        )
    END AS delivery_delay_days
FROM deduplicated;
