-- include/sql/etl/etl_reports.sql
-- Report tables rebuilt from the ETL star schema at the end of every run.

CREATE OR REPLACE TABLE etl_daily_zone_revenue AS
SELECT f.order_date,
       s.zone,
       f.store_id,
       s.store_name,
       count(*) FILTER (WHERE f.order_status = 'DELIVERED') AS delivered_orders,
       sum(f.net_revenue)                                   AS net_revenue
FROM etl_fact_orders f
JOIN etl_dim_store s USING (store_id)
GROUP BY ALL
ORDER BY f.order_date, s.zone, f.store_id;

CREATE OR REPLACE TABLE etl_category_margin AS
SELECT p.category,
       count(*) FILTER (WHERE f.order_status = 'DELIVERED')            AS delivered_orders,
       sum(f.quantity) FILTER (WHERE f.order_status = 'DELIVERED')     AS units,
       sum(f.net_revenue)                                              AS net_revenue,
       sum(f.cost_amount) FILTER (WHERE f.order_status = 'DELIVERED')  AS cost_amount,
       sum(f.gross_margin)                                             AS gross_margin,
       round(100 * sum(f.gross_margin) / nullif(sum(f.net_revenue), 0), 2) AS margin_pct
FROM etl_fact_orders f
JOIN etl_dim_product p USING (product_id)
GROUP BY p.category
ORDER BY net_revenue DESC;
