-- include/sql/elt/dq_orders_table_checks.sql
-- Once-per-run checks on the orders batch: freshness, volume, store capacity.
-- Same definitions as rules.order_table_checks() in the ETL pipeline.

DELETE FROM dq_results
WHERE run_id = (SELECT run_id FROM run_ctx)
  AND dag_id = (SELECT dag_id FROM run_ctx)
  AND check_name IN ('orders.freshness_latest_order', 'orders.volume_vs_last_run', 'orders.store_daily_capacity');

INSERT INTO dq_results
WITH loaded AS (
    SELECT * FROM stg_orders_batch WHERE dq_status <> 'FAILED'
),
freshness AS (
    SELECT 1 AS rows_checked,
           CASE WHEN max(order_ts) IS NULL
                  OR (SELECT run_ts FROM run_ctx) - max(order_ts) > INTERVAL 24 HOUR
                THEN 1 ELSE 0 END AS rows_failed
    FROM loaded
),
volume AS (
    SELECT cur.n AS rows_checked,
           CASE WHEN prev.n > 0 AND cur.n < 0.10 * prev.n THEN 1 ELSE 0 END AS rows_failed
    FROM (SELECT count(*) AS n FROM stg_orders_batch) cur
    LEFT JOIN (
        SELECT rows_checked AS n FROM dq_results
        WHERE dag_id = (SELECT dag_id FROM run_ctx)
          AND run_id <> (SELECT run_id FROM run_ctx)
          AND check_name = 'orders.volume_vs_last_run'
        ORDER BY checked_at DESC
        LIMIT 1
    ) prev ON true
),
store_days AS (
    SELECT l.store_id, CAST(l.order_ts AS DATE) AS order_date, count(*) AS orders,
           any_value(s.capacity_orders_per_day) AS capacity
    FROM loaded l
    LEFT JOIN stg_stores s ON s.store_id = l.store_id AND s.dq_status <> 'FAILED'
    GROUP BY 1, 2
),
capacity AS (
    SELECT count(*) AS rows_checked,
           count(*) FILTER (WHERE capacity IS NOT NULL AND orders > capacity) AS rows_failed
    FROM store_days
),
results AS (
    SELECT 'orders.freshness_latest_order' AS check_name, rows_checked, rows_failed FROM freshness
    UNION ALL SELECT 'orders.volume_vs_last_run', rows_checked, rows_failed FROM volume
    UNION ALL SELECT 'orders.store_daily_capacity', rows_checked, rows_failed FROM capacity
)
SELECT ctx.run_id, ctx.dag_id, r.check_name, c.dimension, c.severity, r.rows_checked, r.rows_failed,
       CASE WHEN r.rows_failed = 0 THEN 'PASS' WHEN c.severity = 'Critical' THEN 'FAIL' ELSE 'WARN' END,
       ctx.checked_at
FROM results r
JOIN dq_check_catalog c USING (check_name)
CROSS JOIN run_ctx ctx;
