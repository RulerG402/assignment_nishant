-- include/sql/elt/mart_category_margin.sql
-- Revenue and gross margin by product category (business question 2).
-- margin = revenue - quantity x cost_price, delivered orders only.

CREATE OR REPLACE TABLE mart_category_margin AS
SELECT category,
       count(*) FILTER (WHERE order_status = 'DELIVERED')            AS delivered_orders,
       sum(quantity) FILTER (WHERE order_status = 'DELIVERED')       AS units,
       sum(net_revenue)                                              AS net_revenue,
       sum(cost_amount) FILTER (WHERE order_status = 'DELIVERED')    AS cost_amount,
       sum(gross_margin)                                             AS gross_margin,
       round(100 * sum(gross_margin) / nullif(sum(net_revenue), 0), 2) AS margin_pct
FROM mart_order_facts
GROUP BY category
ORDER BY net_revenue DESC;
