-- include/sql/etl/etl_scd2.sql
-- Bonus: Type 2 customer dimension that keeps loyalty-tier history.
-- Reads etl_dim_customer (latest snapshot) and scd2_ctx (run_id, run_ts), both
-- prepared by etl.apply_scd2(). Idempotent: running it twice on the same
-- snapshot changes nothing.

-- 1. Close the current row of every customer whose loyalty tier changed.
UPDATE etl_dim_customer_scd2
SET valid_to = (SELECT run_ts FROM scd2_ctx),
    is_current = false
WHERE is_current
  AND customer_id IN (
      SELECT d.customer_id
      FROM etl_dim_customer d
      JOIN etl_dim_customer_scd2 s ON s.customer_id = d.customer_id AND s.is_current
      WHERE s.loyalty_tier IS DISTINCT FROM d.loyalty_tier
  );

-- 2. Open a new current row for customers that have none: brand-new customers
--    (valid from their signup date) and customers whose row was just closed.
INSERT INTO etl_dim_customer_scd2
SELECT d.customer_id, d.customer_name_masked, d.email_masked, d.phone_masked, d.city, d.area, d.pincode,
       d.signup_date, d.loyalty_tier, d.is_active,
       CASE WHEN EXISTS (SELECT 1 FROM etl_dim_customer_scd2 s WHERE s.customer_id = d.customer_id)
            THEN (SELECT run_ts FROM scd2_ctx)
            ELSE CAST(d.signup_date AS TIMESTAMP) END AS valid_from,
       NULL AS valid_to,
       true AS is_current,
       (SELECT run_id FROM scd2_ctx) AS _loaded_run_id
FROM etl_dim_customer d
WHERE NOT EXISTS (
    SELECT 1 FROM etl_dim_customer_scd2 s WHERE s.customer_id = d.customer_id AND s.is_current
);
