-- include/sql/elt/dq_record_rows.sql
-- Template: writes one dq_results row per row-level check of {table}, counted
-- from the crit_failed / warn_failed lists in {relation}. {table} and
-- {relation} are substituted by elt.py from a fixed allow-list, never user input.

DELETE FROM dq_results
WHERE run_id = (SELECT run_id FROM run_ctx)
  AND dag_id = (SELECT dag_id FROM run_ctx)
  AND check_name IN (SELECT check_name FROM dq_check_catalog WHERE table_name = '{table}' AND level = 'row');

INSERT INTO dq_results
SELECT ctx.run_id,
       ctx.dag_id,
       c.check_name,
       c.dimension,
       c.severity,
       (SELECT count(*) FROM {relation})                         AS rows_checked,
       coalesce(f.failed, 0)                                     AS rows_failed,
       CASE WHEN coalesce(f.failed, 0) = 0 THEN 'PASS'
            WHEN c.severity = 'Critical' THEN 'FAIL' ELSE 'WARN' END AS status,
       ctx.checked_at
FROM dq_check_catalog c
CROSS JOIN run_ctx ctx
LEFT JOIN (
    SELECT check_name, count(*) AS failed
    FROM (SELECT unnest(list_concat(crit_failed, warn_failed)) AS check_name FROM {relation})
    GROUP BY check_name
) f USING (check_name)
WHERE c.table_name = '{table}' AND c.level = 'row';
