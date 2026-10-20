-- include/sql/elt/stg_payments.sql
-- Raw gateway export -> typed, deduplicated, quality-flagged snapshot.
-- Orphans and amount mismatches are Warnings: they load here and surface in
-- mart_payment_reconciliation, where finance works them.
-- Requires stg_orders_batch from this run.

CREATE OR REPLACE TABLE stg_payments AS
WITH order_ref AS (
    SELECT * EXCLUDE (pick)
    FROM (
        SELECT order_id, order_ts, order_amount, payment_mode,
               row_number() OVER (PARTITION BY order_id ORDER BY _source_priority DESC, _row_num) AS pick
        FROM stg_orders_batch
        WHERE order_id IS NOT NULL
    )
    WHERE pick = 1
),
typed AS (
    SELECT r._source_file, r._row_num, r._loaded_at,
           r.payment_mode                                          AS mode_raw,
           nullif(trim(r.payment_id), '')                          AS payment_id,
           nullif(trim(r.order_id), '')                            AS order_id,
           try_strptime(trim(r.payment_ts), '%Y-%m-%d %H:%M:%S')   AS payment_ts,
           CASE lower(nullif(trim(r.payment_mode), ''))
               WHEN 'upi' THEN 'UPI' WHEN 'card' THEN 'Card' WHEN 'wallet' THEN 'Wallet'
               WHEN 'cash on delivery' THEN 'Cash on Delivery' WHEN 'cod' THEN 'Cash on Delivery'
               WHEN 'cashh' THEN 'Cash on Delivery'
               ELSE lower(nullif(trim(r.payment_mode), '')) END    AS payment_mode,
           TRY_CAST(nullif(trim(r.amount_paid), '') AS DOUBLE)     AS amount_paid,
           upper(nullif(trim(r.payment_status), ''))               AS payment_status,
           nullif(trim(r.gateway_ref), '')                         AS gateway_ref
    FROM cur_raw_payments r
),
ranked AS (
    SELECT t.*,
           row_number() OVER (PARTITION BY t.payment_id ORDER BY t._row_num)               AS key_rank,
           coalesce(t.payment_mode IN ('UPI', 'Card', 'Cash on Delivery', 'Wallet'), false) AS mode_valid,
           o.order_id IS NOT NULL                                                          AS order_known,
           CAST(o.order_amount AS DOUBLE)                                                  AS order_amount,
           o.payment_mode                                                                  AS order_mode,
           o.order_ts                                                                      AS order_ts
    FROM typed t
    LEFT JOIN order_ref o ON o.order_id = t.order_id
),
checked AS (
    SELECT ranked.*,
        list_filter([
            CASE WHEN payment_id IS NULL THEN 'payments.payment_id_not_null' END,
            CASE WHEN payment_id IS NOT NULL AND key_rank > 1 THEN 'payments.payment_id_unique' END,
            CASE WHEN payment_ts IS NULL THEN 'payments.payment_ts_valid' END,
            CASE WHEN NOT mode_valid THEN 'payments.payment_mode_valid' END,
            CASE WHEN NOT coalesce(payment_status IN ('SUCCESS', 'FAILED', 'REFUNDED'), false)
                 THEN 'payments.payment_status_valid' END,
            CASE WHEN NOT coalesce(amount_paid > 0, false) THEN 'payments.amount_paid_positive' END,
            CASE WHEN payment_ts > least((SELECT run_ts FROM run_ctx), (SELECT payments_cutoff FROM run_ctx))
                 THEN 'payments.payment_ts_not_future' END
        ], x -> x IS NOT NULL) AS crit_failed,
        list_filter([
            CASE WHEN coalesce(payment_mode <> 'Cash on Delivery', true) AND gateway_ref IS NULL
                 THEN 'payments.gateway_ref_present' END,
            CASE WHEN NOT order_known THEN 'payments.order_id_exists' END,
            CASE WHEN payment_status = 'SUCCESS' AND abs(amount_paid - order_amount) > 0.01
                 THEN 'payments.success_amount_matches_order' END,
            CASE WHEN order_known AND mode_valid AND order_mode IS NOT NULL AND payment_mode <> order_mode
                 THEN 'payments.payment_mode_matches_order' END,
            CASE WHEN mode_valid AND mode_raw <> payment_mode THEN 'payments.payment_mode_casing' END,
            CASE WHEN payment_ts < order_ts THEN 'payments.payment_after_order' END
        ], x -> x IS NOT NULL) AS warn_failed
    FROM ranked
)
SELECT payment_id,
       order_id,
       payment_ts,
       payment_mode,
       TRY_CAST(amount_paid AS DECIMAL(12, 2))   AS amount_paid,
       payment_status,
       gateway_ref,
       CASE WHEN len(crit_failed) > 0 THEN 'FAILED'
            WHEN len(warn_failed) > 0 THEN 'WARNING' ELSE 'PASSED' END AS dq_status,
       nullif(array_to_string(crit_failed, '; '), '') AS failure_reason,
       nullif(array_to_string(warn_failed, '; '), '') AS dq_warnings,
       crit_failed,
       warn_failed,
       _source_file,
       _row_num,
       _loaded_at
FROM checked
ORDER BY _row_num;

DELETE FROM stg_dq_failures
WHERE run_id = (SELECT run_id FROM run_ctx) AND source_table = 'payments';

INSERT INTO stg_dq_failures
SELECT ctx.run_id, 'payments', s._source_file, r.payment_id, s._row_num, s.failure_reason,
       CAST(to_json(r) AS VARCHAR), ctx.checked_at
FROM stg_payments s
JOIN cur_raw_payments r ON r._source_file = s._source_file AND r._row_num = s._row_num
CROSS JOIN run_ctx ctx
WHERE s.dq_status = 'FAILED';
