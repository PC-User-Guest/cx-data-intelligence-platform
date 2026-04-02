/* @bruin

name: enrichment.high_risk_customers
type: bq.sql
materialization:
  type: table
depends:
    - core.fact_sentiment_daily
    - staging.stg_orders

@bruin */

-- Identifies customers at risk based on combined sentiment and operational delays.
WITH sentiment_daily AS (
    SELECT
        customer_id,
        sentiment_date,
        avg_sentiment_score,
        ticket_count
    FROM `core.fact_sentiment_daily`
),
order_daily AS (
    SELECT
        customer_id,
        DATE(order_purchase_timestamp) AS order_date,
        AVG(COALESCE(delivery_delay_days, 0)) AS avg_delivery_delay_days,
        COUNTIF(is_delayed_order) AS delayed_order_count,
        COUNT(*) AS order_count
    FROM `staging.stg_orders`
    GROUP BY customer_id, order_date
),
joined AS (
    SELECT
        s.customer_id,
        s.sentiment_date,
        s.avg_sentiment_score,
        s.ticket_count,
        COALESCE(o.avg_delivery_delay_days, 0) AS avg_delivery_delay_days,
        COALESCE(o.delayed_order_count, 0) AS delayed_order_count,
        COALESCE(o.order_count, 0) AS order_count,
        -- Weighted risk score: sentiment pressure plus operational delay pressure.
        (ABS(LEAST(s.avg_sentiment_score, 0)) * 0.7) +
        (LEAST(COALESCE(o.avg_delivery_delay_days, 0), 10) / 10.0 * 0.3) AS risk_score
    FROM sentiment_daily s
    LEFT JOIN order_daily o
        ON s.customer_id = o.customer_id
       AND s.sentiment_date = o.order_date
)
SELECT
    customer_id,
    sentiment_date AS snapshot_date,
    avg_sentiment_score,
    avg_delivery_delay_days,
    delayed_order_count,
    order_count,
    ticket_count,
    risk_score,
    CASE
        WHEN risk_score >= 0.65 THEN 'critical'
        WHEN risk_score >= 0.45 THEN 'high'
        ELSE 'medium'
    END AS risk_band,
    CURRENT_TIMESTAMP() AS refreshed_at
FROM joined
WHERE avg_sentiment_score <= -0.20
  AND avg_delivery_delay_days >= 2;
