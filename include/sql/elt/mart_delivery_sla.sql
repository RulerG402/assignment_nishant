-- include/sql/elt/mart_delivery_sla.sql
-- The "10-minute promise" KPI per store (business question 3). Impossible
-- delivery times (> 120 min) never reach this table: staging rejected them.

CREATE OR REPLACE TABLE mart_delivery_sla AS
SELECT store_id,
       store_name,
       zone,
       count(*)                                          AS delivered_orders,
       round(avg(delivery_minutes), 2)                   AS avg_delivery_minutes,
       count(*) FILTER (WHERE within_15_min)             AS within_15_min_orders,
       round(100.0 * count(*) FILTER (WHERE within_15_min) / count(*), 1) AS pct_within_15_min
FROM mart_order_facts
WHERE order_status = 'DELIVERED'
GROUP BY ALL
ORDER BY store_id;
