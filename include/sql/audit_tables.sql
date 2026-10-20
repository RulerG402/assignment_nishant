-- include/sql/audit_tables.sql
-- Observability tables shared by both NammaMart DAGs. Timestamps are IST wall-clock.

CREATE TABLE IF NOT EXISTS dq_results (
    run_id        VARCHAR,
    dag_id        VARCHAR,
    check_name    VARCHAR,
    dimension     VARCHAR,
    severity      VARCHAR,
    rows_checked  BIGINT,
    rows_failed   BIGINT,
    status        VARCHAR,     -- PASS | WARN (warning failed) | FAIL (critical failed)
    checked_at    TIMESTAMP
);

CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id            VARCHAR,
    dag_id            VARCHAR,
    start_time        TIMESTAMP,
    end_time          TIMESTAMP,
    rows_extracted    BIGINT,  -- every source row read this run
    rows_loaded       BIGINT,  -- order rows that passed and were merged into the fact / staging
    rows_quarantined  BIGINT,  -- rows set aside with a failure_reason
    status            VARCHAR, -- success | failed
    rows_inserted     BIGINT,  -- new orders added by the merge
    rows_updated      BIGINT,  -- existing orders changed by the merge (e.g. late RETURNED)
    failed_step       VARCHAR  -- first task that produced no result, when status = failed
);
