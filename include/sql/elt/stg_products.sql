-- include/sql/elt/stg_products.sql
-- Raw product catalogue -> typed, deduplicated, quality-flagged snapshot.
-- Duplicate product_ids: the '(old)' catalogue row loses to the current one.

CREATE OR REPLACE TABLE stg_products AS
WITH typed AS (
    SELECT r._source_file, r._row_num, r._loaded_at,
           nullif(trim(r.product_id), '')                          AS product_id,
           nullif(trim(r.product_name), '')                        AS product_name,
           nullif(trim(r.category), '')                            AS category,
           nullif(trim(r.sub_category), '')                        AS sub_category,
           nullif(trim(r.brand), '')                               AS brand,
           TRY_CAST(nullif(trim(r.mrp), '') AS DOUBLE)             AS mrp,
           TRY_CAST(nullif(trim(r.cost_price), '') AS DOUBLE)      AS cost_price,
           nullif(trim(r.unit), '')                                AS unit,
           lower(nullif(trim(r.is_perishable), ''))                AS is_perishable_text,
           CAST(try_strptime(trim(r.launch_date), '%Y-%m-%d') AS DATE) AS launch_date
    FROM cur_raw_products r
),
ranked AS (
    SELECT *,
           row_number() OVER (
               PARTITION BY product_id
               ORDER BY CASE WHEN coalesce(ends_with(product_name, '(old)'), false) THEN 1 ELSE 0 END, _row_num
           ) AS key_rank
    FROM typed
),
checked AS (
    SELECT ranked.*,
        list_filter([
            CASE WHEN product_id IS NULL THEN 'products.product_id_not_null' END,
            CASE WHEN category IS NULL THEN 'products.category_not_null' END,
            CASE WHEN product_id IS NOT NULL AND key_rank > 1 THEN 'products.product_id_unique' END,
            CASE WHEN category IS NOT NULL AND category NOT IN (
                     'Fruits & Vegetables', 'Dairy & Bakery', 'Staples',
                     'Snacks & Beverages', 'Personal Care', 'Household')
                 THEN 'products.category_valid' END,
            CASE WHEN NOT coalesce(mrp > 0, false) THEN 'products.mrp_positive' END,
            CASE WHEN NOT coalesce(cost_price <= mrp, false) THEN 'products.cost_price_le_mrp' END,
            CASE WHEN CAST(launch_date AS TIMESTAMP) > (SELECT run_ts FROM run_ctx)
                 THEN 'products.launch_date_not_future' END
        ], x -> x IS NOT NULL) AS crit_failed,
        list_filter([
            CASE WHEN NOT coalesce(is_perishable_text IN ('true', 'false'), false)
                 THEN 'products.is_perishable_boolean' END
        ], x -> x IS NOT NULL) AS warn_failed
    FROM ranked
)
SELECT product_id,
       product_name,
       category,
       sub_category,
       brand,
       CAST(mrp AS DECIMAL(12, 2))          AS mrp,
       CAST(cost_price AS DECIMAL(12, 2))   AS cost_price,
       unit,
       TRY_CAST(is_perishable_text AS BOOLEAN) AS is_perishable,
       launch_date,
       key_rank = 1                          AS is_current_row,
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
WHERE run_id = (SELECT run_id FROM run_ctx) AND source_table = 'products';

INSERT INTO stg_dq_failures
SELECT ctx.run_id, 'products', s._source_file, r.product_id, s._row_num, s.failure_reason,
       CAST(to_json(r) AS VARCHAR), ctx.checked_at
FROM stg_products s
JOIN cur_raw_products r ON r._source_file = s._source_file AND r._row_num = s._row_num
CROSS JOIN run_ctx ctx
WHERE s.dq_status = 'FAILED';
