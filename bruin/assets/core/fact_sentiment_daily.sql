/* @bruin

name: core.fact_sentiment_daily
type: bq.sql
materialization:
  type: table
depends:
    - staging.stg_tickets

@bruin */

-- Aggregates customer sentiment at daily grain for KPI tracking and risk scoring.
WITH ticket_base AS (
    SELECT
        customer_id,
        DATE(created_at) AS sentiment_date,
        sentiment_score
    FROM `staging.stg_tickets`
),
aggregated AS (
    SELECT
        customer_id,
        sentiment_date,
        AVG(sentiment_score) AS avg_sentiment_score,
        COUNT(*) AS ticket_count,
        COUNTIF(sentiment_score < 0) AS negative_ticket_count
    FROM ticket_base
    GROUP BY customer_id, sentiment_date
)
SELECT
    customer_id,
    sentiment_date,
    avg_sentiment_score,
    ticket_count,
    negative_ticket_count,
    SAFE_DIVIDE(negative_ticket_count, ticket_count) AS negative_ticket_ratio
FROM aggregated;
