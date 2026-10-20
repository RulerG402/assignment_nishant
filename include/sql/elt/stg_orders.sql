-- include/sql/elt/stg_orders.sql
-- 1. stg_orders_batch : every order row loaded this run (backfill + increments),
--                       typed, standardised and checked.
-- 2. stg_dq_failures  : rejected rows, with the raw record.
-- 3. MERGE            : upsert the best version of each order into stg_orders.
-- Requires stg_stores, stg_products and stg_customers from this run.

CREATE OR REPLACE TABLE stg_orders_batch AS
WITH typed AS (
    SELECT r._source_file, r._row_num, r._loaded_at,
           src.priority                                               AS _source_priority,
           src.cutoff                                                 AS _cutoff,
           r.order_status                                             AS status_raw,
           r.payment_mode                                             AS mode_raw,
           nullif(trim(r.order_id), '')                               AS order_id,
           try_strptime(trim(r.order_ts), '%Y-%m-%d %H:%M:%S')        AS order_ts,
           nullif(trim(r.customer_id), '')                            AS customer_id,
           nullif(trim(r.store_id), '')                               AS store_id,
           nullif(trim(r.product_id), '')                             AS product_id,
           TRY_CAST(nullif(trim(r.quantity), '') AS DOUBLE)           AS quantity,
           TRY_CAST(nullif(trim(r.unit_price), '') AS DOUBLE)         AS unit_price,
           TRY_CAST(nullif(trim(r.discount_pct), '') AS DOUBLE)       AS discount_pct,
           TRY_CAST(nullif(trim(r.order_amount), '') AS DOUBLE)       AS order_amount,
           CASE lower(nullif(trim(r.payment_mode), ''))
               WHEN 'upi' THEN 'UPI' WHEN 'card' THEN 'Card' WHEN 'wallet' THEN 'Wallet'
               WHEN 'cash on delivery' THEN 'Cash on Delivery' WHEN 'cod' THEN 'Cash on Delivery'
               WHEN 'cashh' THEN 'Cash on Delivery'
               ELSE lower(nullif(trim(r.payment_mode), '')) END       AS payment_mode,
           upper(nullif(trim(r.order_status), ''))                    AS order_status,
           nullif(trim(r.channel), '')                                AS channel,
           TRY_CAST(nullif(trim(r.delivery_minutes), '') AS DOUBLE)   AS delivery_minutes
    FROM cur_raw_orders r
    JOIN order_sources src ON src.source_file = r._source_file
),
refs AS (
    SELECT t.*,
        row_number() OVER (PARTITION BY t._source_file, t.order_id ORDER BY t._row_num) AS key_rank,
        coalesce(t.order_status IN ('DELIVERED', 'CANCELLED', 'RETURNED'), false)        AS status_valid,
        coalesce(t.payment_mode IN ('UPI', 'Card', 'Cash on Delivery', 'Wallet'), false) AS mode_valid,
        p.mrp                                                                            AS product_mrp,
        t.customer_id IN (SELECT customer_id FROM stg_customers WHERE customer_id IS NOT NULL) AS customer_exists,
        t.product_id  IN (SELECT product_id  FROM stg_products  WHERE product_id  IS NOT NULL) AS product_exists,
        t.store_id    IN (SELECT store_id    FROM stg_stores    WHERE store_id    IS NOT NULL) AS store_exists,
        t.customer_id IN (SELECT customer_id FROM stg_customers WHERE dq_status <> 'FAILED') AS customer_clean,
        t.product_id  IN (SELECT product_id  FROM stg_products  WHERE dq_status <> 'FAILED') AS product_clean,
        t.store_id    IN (SELECT store_id    FROM stg_stores    WHERE dq_status <> 'FAILED') AS store_clean
    FROM typed t
    LEFT JOIN stg_products p ON p.product_id = t.product_id AND p.is_current_row
),
checked AS (
    SELECT refs.*,
        list_filter([
            CASE WHEN order_id IS NULL THEN 'orders.order_id_not_null' END,
            CASE WHEN customer_id IS NULL THEN 'orders.customer_id_not_null' END,
            CASE WHEN order_status = 'DELIVERED' AND delivery_minutes IS NULL
                 THEN 'orders.delivered_has_delivery_minutes' END,
            CASE WHEN order_id IS NOT NULL AND key_rank > 1 THEN 'orders.order_id_unique' END,
            CASE WHEN order_ts IS NULL THEN 'orders.order_ts_valid' END,
            CASE WHEN NOT status_valid THEN 'orders.order_status_valid' END,
            CASE WHEN NOT mode_valid THEN 'orders.payment_mode_valid' END,
            CASE WHEN NOT coalesce(discount_pct BETWEEN 0 AND 100, false) THEN 'orders.discount_pct_in_range' END,
            CASE WHEN NOT coalesce(quantity > 0, false) THEN 'orders.quantity_positive' END,
            CASE WHEN NOT coalesce(abs(quantity * unit_price * (1 - discount_pct / 100) - order_amount) <= 0.01, false)
                 THEN 'orders.order_amount_matches_formula' END,
            CASE WHEN delivery_minutes > 120 THEN 'orders.delivery_minutes_le_120' END,
            CASE WHEN NOT coalesce(customer_exists, false) THEN 'orders.customer_id_exists' END,
            CASE WHEN NOT coalesce(product_exists, false) THEN 'orders.product_id_exists' END,
            CASE WHEN NOT coalesce(store_exists, false) THEN 'orders.store_id_exists' END,
            CASE WHEN coalesce(customer_exists, false) AND NOT coalesce(customer_clean, false)
                 THEN 'orders.customer_master_clean' END,
            CASE WHEN coalesce(product_exists, false) AND NOT coalesce(product_clean, false)
                 THEN 'orders.product_master_clean' END,
            CASE WHEN coalesce(store_exists, false) AND NOT coalesce(store_clean, false)
                 THEN 'orders.store_master_clean' END,
            CASE WHEN order_ts > least(_cutoff, (SELECT run_ts FROM run_ctx)) THEN 'orders.order_ts_not_future' END
        ], x -> x IS NOT NULL) AS crit_failed,
        list_filter([
            CASE WHEN delivery_minutes IS NOT NULL AND NOT (delivery_minutes BETWEEN 8 AND 35)
                 THEN 'orders.delivery_minutes_typical_range' END,
            CASE WHEN product_mrp IS NOT NULL AND unit_price > product_mrp THEN 'orders.unit_price_le_mrp' END,
            CASE WHEN status_valid AND status_raw <> order_status THEN 'orders.order_status_casing' END,
            CASE WHEN mode_valid AND mode_raw <> payment_mode THEN 'orders.payment_mode_casing' END
        ], x -> x IS NOT NULL) AS warn_failed
    FROM refs
),
final AS (
    SELECT order_id,
           order_ts,
           CAST(order_ts AS DATE)                      AS order_date,
           customer_id,
           store_id,
           product_id,
           TRY_CAST(quantity AS INTEGER)               AS quantity,
           TRY_CAST(unit_price AS DECIMAL(12, 2))      AS unit_price,
           TRY_CAST(discount_pct AS DECIMAL(6, 2))     AS discount_pct,
           TRY_CAST(order_amount AS DECIMAL(12, 2))    AS order_amount,
           payment_mode,
           order_status,
           channel,
           TRY_CAST(delivery_minutes AS INTEGER)       AS delivery_minutes,
           CASE WHEN len(crit_failed) > 0 THEN 'FAILED'
                WHEN len(warn_failed) > 0 THEN 'WARNING' ELSE 'PASSED' END AS dq_status,
           nullif(array_to_string(crit_failed, '; '), '') AS failure_reason,
           nullif(array_to_string(warn_failed, '; '), '') AS dq_warnings,
           crit_failed,
           warn_failed,
           _source_file,
           _source_priority,
           _row_num,
           _loaded_at
    FROM checked
)
SELECT final.*,
       md5(concat_ws('|', coalesce(CAST(order_ts AS VARCHAR), ''), coalesce(customer_id, ''),
                     coalesce(store_id, ''), coalesce(product_id, ''), coalesce(CAST(quantity AS VARCHAR), ''),
                     coalesce(CAST(unit_price AS VARCHAR), ''), coalesce(CAST(discount_pct AS VARCHAR), ''),
                     coalesce(CAST(order_amount AS VARCHAR), ''), coalesce(payment_mode, ''),
                     coalesce(order_status, ''), coalesce(channel, ''),
                     coalesce(CAST(delivery_minutes AS VARCHAR), ''))) AS _row_hash
FROM final
ORDER BY _source_priority, _row_num;

-- ── quarantine ─────────────────────────────────────────────────────────────
DELETE FROM stg_dq_failures
WHERE run_id = (SELECT run_id FROM run_ctx) AND source_table = 'orders';

INSERT INTO stg_dq_failures
SELECT ctx.run_id, 'orders', b._source_file, r.order_id, b._row_num, b.failure_reason,
       CAST(to_json(r) AS VARCHAR), ctx.checked_at
FROM stg_orders_batch b
JOIN cur_raw_orders r ON r._source_file = b._source_file AND r._row_num = b._row_num
CROSS JOIN run_ctx ctx
WHERE b.dq_status = 'FAILED';

-- ── merge ──────────────────────────────────────────────────────────────────
-- One candidate per order: a passing version beats a failing one, then the
-- newest file wins. Exact in-file duplicates never compete.
CREATE OR REPLACE TEMP TABLE order_candidates AS
SELECT * EXCLUDE (pick)
FROM (
    SELECT b.*,
           row_number() OVER (PARTITION BY order_id
                              ORDER BY dq_status = 'FAILED', _source_priority DESC, _row_num) AS pick
    FROM stg_orders_batch b
    WHERE order_id IS NOT NULL AND NOT list_contains(crit_failed, 'orders.order_id_unique')
)
WHERE pick = 1;

CREATE TABLE IF NOT EXISTS stg_orders_merge_log (
    run_id VARCHAR, rows_inserted BIGINT, rows_updated BIGINT, rows_merged BIGINT, merged_at TIMESTAMP
);
DELETE FROM stg_orders_merge_log WHERE run_id = (SELECT run_id FROM run_ctx);

-- "inserted" / "updated" count changes to the set of orders marts can see.
INSERT INTO stg_orders_merge_log
SELECT ctx.run_id,
       count(*) FILTER (WHERE c.dq_status <> 'FAILED' AND (t.order_id IS NULL OR t.dq_status = 'FAILED')),
       count(*) FILTER (WHERE c.dq_status <> 'FAILED' AND t.dq_status <> 'FAILED'
                        AND c._source_priority >= t._source_priority AND c._row_hash <> t._row_hash),
       count(*) FILTER (WHERE c.dq_status <> 'FAILED'),
       ctx.checked_at
FROM order_candidates c
LEFT JOIN stg_orders t USING (order_id)
CROSS JOIN run_ctx ctx
GROUP BY ctx.run_id, ctx.checked_at;

MERGE INTO stg_orders AS t
USING (
    SELECT c.order_id, c.order_ts, c.order_date, c.customer_id, c.store_id, c.product_id, c.quantity,
           c.unit_price, c.discount_pct, c.order_amount, c.payment_mode, c.order_status, c.channel,
           c.delivery_minutes, c.dq_status, c.failure_reason, c.dq_warnings, c._source_file,
           c._source_priority, c._row_num, c._row_hash, ctx.run_id AS _run_id, ctx.checked_at AS _merged_at
    FROM order_candidates c CROSS JOIN run_ctx ctx
) AS s
ON t.order_id = s.order_id
WHEN MATCHED AND (
        (s.dq_status <> 'FAILED' AND (t.dq_status = 'FAILED' OR s._source_priority >= t._source_priority))
     OR (s.dq_status = 'FAILED' AND t.dq_status = 'FAILED' AND s._source_priority >= t._source_priority)
    ) THEN UPDATE
WHEN NOT MATCHED THEN INSERT;
