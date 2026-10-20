-- include/sql/elt/mart_payment_reconciliation.sql
-- One row per order/payment mismatch (business question 4, acceptance #7).
-- Same rules as include/nammamart/reconcile.py. unreconciled_amount is the money
-- to chase for that row, so it sums without double counting.

CREATE OR REPLACE TABLE mart_payment_reconciliation AS
WITH orders AS (
    SELECT order_id, order_status, CAST(order_amount AS DOUBLE) AS order_amount, payment_mode
    FROM stg_orders
    WHERE dq_status <> 'FAILED'
),
pays AS (
    SELECT payment_id, order_id, payment_status, payment_mode, CAST(amount_paid AS DOUBLE) AS amount_paid
    FROM stg_payments
    WHERE dq_status <> 'FAILED'
),
per_order AS (
    SELECT order_id,
           count(*) FILTER (WHERE payment_status = 'SUCCESS')               AS success_count,
           count(*) FILTER (WHERE payment_status = 'REFUNDED')              AS refund_count,
           count(*) FILTER (WHERE payment_status = 'FAILED')                AS failed_count,
           coalesce(sum(amount_paid) FILTER (WHERE payment_status = 'SUCCESS'), 0) AS success_amount
    FROM pays
    GROUP BY order_id
),
mode_mismatch AS (
    SELECT DISTINCT p.order_id
    FROM pays p JOIN orders o USING (order_id)
    WHERE p.payment_status = 'SUCCESS' AND p.payment_mode <> o.payment_mode
),
agg AS (
    SELECT o.*,
           coalesce(p.success_count, 0)  AS success_count,
           coalesce(p.refund_count, 0)   AS refund_count,
           coalesce(p.failed_count, 0)   AS failed_count,
           coalesce(p.success_amount, 0) AS success_amount,
           round(abs(coalesce(p.success_amount, 0) - o.order_amount), 2) AS diff,
           m.order_id IS NOT NULL        AS mode_mismatch
    FROM orders o
    LEFT JOIN per_order p USING (order_id)
    LEFT JOIN mode_mismatch m ON m.order_id = o.order_id
),
order_rules AS (
    SELECT a.*, r.reason, r.unreconciled_amount
    FROM agg a,
    LATERAL (
        SELECT * FROM (VALUES
            ('missing_payment',
             a.order_status = 'DELIVERED' AND a.success_count = 0 AND a.failed_count = 0, a.order_amount),
            ('payment_failed',
             a.order_status = 'DELIVERED' AND a.success_count = 0 AND a.failed_count > 0, a.order_amount),
            ('amount_mismatch',
             a.order_status = 'DELIVERED' AND a.success_count > 0 AND a.diff > 0.01, a.diff),
            ('multiple_success_payments', a.success_count > 1, 0.0),
            ('payment_mode_mismatch', a.mode_mismatch, 0.0),
            ('returned_not_refunded',
             a.order_status = 'RETURNED' AND a.success_count > 0 AND a.refund_count = 0, a.success_amount),
            ('payment_on_cancelled_order',
             a.order_status = 'CANCELLED' AND a.success_count > 0, a.success_amount)
        ) v(reason, hit, unreconciled_amount)
        WHERE hit
    ) r
),
order_rows AS (
    SELECT order_id,
           CAST(NULL AS VARCHAR)                                   AS payment_id,
           order_status,
           CAST(NULL AS VARCHAR)                                   AS payment_status,
           order_amount,
           CASE WHEN success_count > 0 THEN success_amount END     AS amount_paid,
           unreconciled_amount,
           reason                                                  AS mismatch_reason
    FROM order_rules
),
payment_rows AS (
    SELECT p.order_id,
           p.payment_id,
           CAST(NULL AS VARCHAR)                                   AS order_status,
           p.payment_status,
           CAST(NULL AS DOUBLE)                                    AS order_amount,
           p.amount_paid,
           CASE WHEN p.payment_status = 'SUCCESS' THEN p.amount_paid ELSE 0 END AS unreconciled_amount,
           CASE WHEN EXISTS (SELECT 1 FROM stg_orders_batch b WHERE b.order_id = p.order_id)
                  OR EXISTS (SELECT 1 FROM stg_orders s WHERE s.order_id = p.order_id)
                THEN 'order_not_in_finance_tables' ELSE 'orphan_payment' END AS mismatch_reason
    FROM pays p
    WHERE NOT EXISTS (SELECT 1 FROM orders o WHERE o.order_id = p.order_id)
)
SELECT u.order_id, u.payment_id, u.order_status, u.payment_status, u.order_amount, u.amount_paid,
       round(u.unreconciled_amount, 2) AS unreconciled_amount, u.mismatch_reason,
       ctx.run_id, ctx.checked_at AS reconciled_at
FROM (SELECT * FROM order_rows UNION ALL SELECT * FROM payment_rows) u
CROSS JOIN run_ctx ctx
ORDER BY u.mismatch_reason, u.order_id NULLS FIRST, u.payment_id NULLS FIRST;
