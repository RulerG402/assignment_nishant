-- include/sql/elt/stg_stores.sql
-- Raw store master -> typed, standardised, quality-flagged snapshot.
-- Check order inside each list mirrors include/nammamart/catalog.py.

CREATE OR REPLACE TABLE stg_stores AS
WITH typed AS (
    SELECT r._source_file, r._row_num, r._loaded_at,
           nullif(trim(r.store_id), '')                                   AS store_id,
           nullif(trim(r.store_name), '')                                 AS store_name,
           CASE WHEN nullif(trim(r.zone), '') IS NOT NULL
                THEN upper(left(trim(r.zone), 1)) || lower(substr(trim(r.zone), 2)) END AS zone_std,
           nullif(trim(r.area), '')                                       AS area,
           nullif(trim(r.pincode), '')                                    AS pincode,
           CAST(try_strptime(trim(r.open_date), '%Y-%m-%d') AS DATE)      AS open_date,
           nullif(trim(r.manager_name), '')                               AS manager_name,
           TRY_CAST(nullif(trim(r.capacity_orders_per_day), '') AS DOUBLE) AS capacity
    FROM cur_raw_stores r
),
ranked AS (
    SELECT *, row_number() OVER (PARTITION BY store_id ORDER BY _row_num) AS key_rank
    FROM typed
),
checked AS (
    SELECT ranked.*,
        list_filter([
            CASE WHEN store_id IS NULL THEN 'stores.store_id_not_null' END,
            CASE WHEN store_id IS NOT NULL AND key_rank > 1 THEN 'stores.store_id_unique' END,
            CASE WHEN zone_std IS NOT NULL AND zone_std NOT IN ('North', 'South', 'East', 'West')
                 THEN 'stores.zone_valid' END,
            CASE WHEN open_date IS NULL THEN 'stores.open_date_valid' END,
            CASE WHEN CAST(open_date AS TIMESTAMP) > (SELECT run_ts FROM run_ctx)
                 THEN 'stores.open_date_not_future' END
        ], x -> x IS NOT NULL) AS crit_failed,
        list_filter([
            CASE WHEN zone_std IS NULL THEN 'stores.zone_not_null' END,
            CASE WHEN NOT coalesce(regexp_full_match(pincode, '\d{6}'), false) THEN 'stores.pincode_6_digits' END,
            CASE WHEN NOT coalesce(capacity > 0, false) THEN 'stores.capacity_positive' END
        ], x -> x IS NOT NULL) AS warn_failed
    FROM ranked
)
SELECT store_id,
       store_name,
       coalesce(zone_std, 'Unassigned')        AS zone,
       area,
       pincode,
       open_date,
       manager_name,
       TRY_CAST(capacity AS INTEGER)           AS capacity_orders_per_day,
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
WHERE run_id = (SELECT run_id FROM run_ctx) AND source_table = 'stores';

INSERT INTO stg_dq_failures
SELECT ctx.run_id, 'stores', s._source_file, r.store_id, s._row_num, s.failure_reason,
       CAST(to_json(r) AS VARCHAR), ctx.checked_at
FROM stg_stores s
JOIN cur_raw_stores r ON r._source_file = s._source_file AND r._row_num = s._row_num
CROSS JOIN run_ctx ctx
WHERE s.dq_status = 'FAILED';
