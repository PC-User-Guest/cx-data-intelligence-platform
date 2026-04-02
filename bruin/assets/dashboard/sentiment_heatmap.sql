/* @bruin

name: dashboard.sentiment_heatmap
type: bq.sql
materialization:
    type: table
depends:
    - staging.stg_tickets

@bruin */

-- Dashboard query: sentiment by region and hour/day buckets.
SELECT
    customer_region AS region,
    DATE(created_at) AS sentiment_date,
    EXTRACT(HOUR FROM created_at) AS sentiment_hour,
    AVG(sentiment_score) AS avg_sentiment_score,
    COUNT(*) AS ticket_count
FROM `staging.stg_tickets`
GROUP BY
    customer_region,
    sentiment_date,
    sentiment_hour
ORDER BY
    sentiment_date DESC,
    sentiment_hour DESC,
    region;
