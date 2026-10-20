-- include/sql/elt/mart_daily_store_kpis.sql
-- One row per day x store: the COO's morning numbers (business question 1).

CREATE OR REPLACE TABLE mart_daily_store_kpis AS
SELECT order_date,
       zone,
       store_id,
       store_name,
       count(*)                                                     AS total_orders,
       count(*) FILTER (WHERE order_status = 'DELIVERED')           AS delivered_orders,
       count(*) FILTER (WHERE order_status = 'CANCELLED')           AS cancelled_orders,
       count(*) FILTER (WHERE order_status = 'RETURNED')            AS returned_orders,
       sum(net_revenue)                                             AS net_revenue,
       sum(gross_margin)                                            AS gross_margin,
       round(avg(delivery_minutes) FILTER (WHERE order_status = 'DELIVERED'), 1) AS avg_delivery_minutes,
       round(100.0 * count(*) FILTER (WHERE within_15_min)
             / nullif(count(*) FILTER (WHERE order_status = 'DELIVERED'), 0), 1) AS pct_within_15_min
FROM mart_order_facts
GROUP BY ALL
ORDER BY order_date, zone, store_id;
