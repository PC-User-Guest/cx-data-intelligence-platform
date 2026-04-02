/* @bruin

name: dashboard.high_risk_table
type: bq.sql
materialization:
    type: table
depends:
    - enrichment.high_risk_customers

@bruin */

-- Dashboard query: near-real-time list of highest-risk customers.
SELECT
    customer_id,
    avg_sentiment_score,
    avg_delivery_delay_days,
    delayed_order_count,
    order_count,
    ticket_count,
    risk_score,
    risk_band,
    refreshed_at AS event_timestamp
FROM `enrichment.high_risk_customers`
ORDER BY
    risk_score DESC,
    event_timestamp DESC
LIMIT 500;
