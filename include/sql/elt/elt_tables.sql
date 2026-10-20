-- include/sql/elt/elt_tables.sql
-- Persistent ELT tables that are merged into rather than rebuilt each run.
-- (raw_* tables are created by the load step from the file contracts;
--  the other stg_* and mart_* tables are rebuilt by their own .sql files.)

-- Current version of every order ever received. MERGE target for the backfill
-- and every increment file; failed rows are kept, flagged, and never reach marts.
CREATE TABLE IF NOT EXISTS stg_orders (
    order_id          VARCHAR PRIMARY KEY,
    order_ts          TIMESTAMP,
    order_date        DATE,
    customer_id       VARCHAR,
    store_id          VARCHAR,
    product_id        VARCHAR,
    quantity          INTEGER,
    unit_price        DECIMAL(12, 2),
    discount_pct      DECIMAL(6, 2),
    order_amount      DECIMAL(12, 2),
    payment_mode      VARCHAR,
    order_status      VARCHAR,
    channel           VARCHAR,
    delivery_minutes  INTEGER,
    dq_status         VARCHAR,          -- PASSED | WARNING | FAILED
    failure_reason    VARCHAR,          -- critical checks failed, '; '-separated
    dq_warnings       VARCHAR,          -- warning checks failed, '; '-separated
    _source_file      VARCHAR,
    _source_priority  INTEGER,          -- 0 = September backfill, YYYYMMDD = increment
    _row_num          BIGINT,
    _row_hash         VARCHAR,
    _run_id           VARCHAR,
    _merged_at        TIMESTAMP
);

-- The ELT quarantine: every row a Critical check rejected, per run, with the
-- raw record exactly as it sits in raw_* (restricted: may contain PII).
CREATE TABLE IF NOT EXISTS stg_dq_failures (
    run_id          VARCHAR,
    source_table    VARCHAR,
    source_file     VARCHAR,
    record_key      VARCHAR,
    row_num         BIGINT,
    failure_reason  VARCHAR,
    raw_record      VARCHAR,
    recorded_at     TIMESTAMP
);
