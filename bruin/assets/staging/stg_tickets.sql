/* @bruin

name: staging.stg_tickets
type: bq.sql
materialization:
  type: table
depends:
    - raw_zone.raw_tickets

@bruin */

-- Cleans and deduplicates raw ticket events for downstream analytics.
WITH source_data AS (
    SELECT
        ticket_id,
        customer_id,
        created_at,
        sentiment_score,
        sentiment_label,
        status,
        channel,
        priority,
        customer_region,
        customer_tier,
        _dlt_load_id,
        _dlt_id
    FROM `raw_zone.raw_tickets`
),
casted AS (
    SELECT
        CAST(ticket_id AS STRING) AS ticket_id,
        CAST(customer_id AS STRING) AS customer_id,
        SAFE_CAST(created_at AS TIMESTAMP) AS created_at,
        SAFE_CAST(sentiment_score AS FLOAT64) AS sentiment_score,
        COALESCE(NULLIF(CAST(sentiment_label AS STRING), ''), 'unknown') AS sentiment_label,
        COALESCE(NULLIF(CAST(status AS STRING), ''), 'unknown') AS ticket_status,
        COALESCE(NULLIF(CAST(channel AS STRING), ''), 'unknown') AS channel,
        COALESCE(NULLIF(CAST(priority AS STRING), ''), 'normal') AS priority,
        COALESCE(NULLIF(CAST(customer_region AS STRING), ''), 'unknown') AS customer_region,
        COALESCE(NULLIF(CAST(customer_tier AS STRING), ''), 'unknown') AS customer_tier,
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
                PARTITION BY ticket_id
                ORDER BY _dlt_load_id DESC, created_at DESC, _dlt_id DESC
            ) AS row_num
        FROM casted
        WHERE created_at IS NOT NULL
    )
    WHERE row_num = 1
)
SELECT
    ticket_id,
    customer_id,
    created_at,
    sentiment_score,
    sentiment_label,
    ticket_status,
    channel,
    priority,
    customer_region,
    customer_tier
FROM deduplicated;
