-- include/sql/elt/mart_order_facts.sql
-- Business-ready order grain shared by every mart: passing orders only, joined
-- to passing master rows, with the same derived measures as etl_fact_orders.

CREATE OR REPLACE VIEW mart_order_facts AS
SELECT o.order_id,
       o.order_ts,
       o.order_date,
       o.customer_id,
       o.store_id,
       o.product_id,
       o.quantity,
       o.unit_price,
       o.discount_pct,
       o.order_amount,
       o.payment_mode,
       o.order_status,
       o.channel,
       o.delivery_minutes,
       s.store_name,
       s.zone,
       p.category,
       c.loyalty_tier,
       CAST(CASE WHEN o.order_status = 'DELIVERED' THEN o.order_amount ELSE 0 END AS DECIMAL(12, 2)) AS net_revenue,
       CAST(round(o.quantity * p.cost_price, 2) AS DECIMAL(12, 2))                                  AS cost_amount,
       CAST(CASE WHEN o.order_status = 'DELIVERED'
                 THEN o.order_amount - round(o.quantity * p.cost_price, 2) ELSE 0 END AS DECIMAL(12, 2)) AS gross_margin,
       CASE WHEN o.order_status = 'DELIVERED' THEN o.delivery_minutes <= 15 END                     AS within_15_min,
       o._source_file
FROM stg_orders o
JOIN stg_stores    s ON s.store_id    = o.store_id    AND s.dq_status <> 'FAILED'
JOIN stg_products  p ON p.product_id  = o.product_id  AND p.dq_status <> 'FAILED'
JOIN stg_customers c ON c.customer_id = o.customer_id AND c.dq_status <> 'FAILED'
WHERE o.dq_status <> 'FAILED';
