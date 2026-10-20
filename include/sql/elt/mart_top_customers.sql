-- include/sql/elt/mart_top_customers.sql
-- Customer spend ranking with loyalty tier (business question 5).
-- Only masked identifiers: no raw name, email or phone exists past staging.

CREATE OR REPLACE TABLE mart_top_customers AS
SELECT dense_rank() OVER (ORDER BY sum(f.net_revenue) DESC) AS spend_rank,
       f.customer_id,
       c.customer_name_masked,
       c.email_masked,
       c.phone_masked,
       c.loyalty_tier,
       count(*) FILTER (WHERE f.order_status = 'DELIVERED')  AS delivered_orders,
       sum(f.net_revenue)                                    AS net_spend
FROM mart_order_facts f
JOIN stg_customers c ON c.customer_id = f.customer_id AND c.dq_status <> 'FAILED'
GROUP BY f.customer_id, c.customer_name_masked, c.email_masked, c.phone_masked, c.loyalty_tier
HAVING sum(f.net_revenue) > 0
ORDER BY spend_rank, f.customer_id;
