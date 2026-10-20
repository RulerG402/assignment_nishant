-- include/sql/etl/etl_tables.sql
-- Star schema for the ETL pipeline (nammamart_etl_nishantdaund).
-- Only rows that passed every Critical check are loaded. No raw PII is stored.

CREATE TABLE IF NOT EXISTS etl_dim_store (
    store_id                 VARCHAR PRIMARY KEY,
    store_name               VARCHAR,
    zone                     VARCHAR,      -- North/South/East/West, 'Unassigned' when the master is blank
    area                     VARCHAR,
    pincode                  VARCHAR,
    open_date                DATE,
    manager_name             VARCHAR,      -- employee name (internal), not customer PII
    capacity_orders_per_day  INTEGER
);

CREATE TABLE IF NOT EXISTS etl_dim_product (
    product_id     VARCHAR PRIMARY KEY,
    product_name   VARCHAR,
    category       VARCHAR,
    sub_category   VARCHAR,
    brand          VARCHAR,
    mrp            DECIMAL(12, 2),
    cost_price     DECIMAL(12, 2),
    unit           VARCHAR,
    is_perishable  BOOLEAN,
    launch_date    DATE
);

CREATE TABLE IF NOT EXISTS etl_dim_customer (
    customer_id           VARCHAR PRIMARY KEY,
    customer_name_masked  VARCHAR,         -- 'Priya H.'
    email_masked          VARCHAR,         -- 'pr***@example.com'; NULL when the source email was invalid
    phone_masked          VARCHAR,         -- '98******21'; NULL when the source phone was invalid
    city                  VARCHAR,
    area                  VARCHAR,
    pincode               VARCHAR,
    signup_date           DATE,
    loyalty_tier          VARCHAR,         -- Bronze/Silver/Gold/Platinum
    is_active             BOOLEAN
);

CREATE TABLE IF NOT EXISTS etl_fact_orders (
    order_id          VARCHAR PRIMARY KEY,
    order_ts          TIMESTAMP,           -- IST
    order_date        DATE,
    customer_id       VARCHAR,
    store_id          VARCHAR,
    product_id        VARCHAR,
    quantity          INTEGER,
    unit_price        DECIMAL(12, 2),
    discount_pct      DECIMAL(6, 2),
    order_amount      DECIMAL(12, 2),
    payment_mode      VARCHAR,
    order_status      VARCHAR,             -- DELIVERED/CANCELLED/RETURNED
    channel           VARCHAR,
    delivery_minutes  INTEGER,
    net_revenue       DECIMAL(12, 2),      -- order_amount when DELIVERED, else 0
    cost_amount       DECIMAL(12, 2),      -- quantity x cost_price
    gross_margin      DECIMAL(12, 2),      -- net_revenue - cost_amount for DELIVERED, else 0
    within_15_min     BOOLEAN,             -- DELIVERED within 15 minutes; NULL otherwise
    _source_file      VARCHAR,             -- export that supplied the current version
    _source_priority  INTEGER,             -- 0 = September backfill, YYYYMMDD = increment
    _row_hash         VARCHAR,             -- md5 of business columns, to count real updates
    _loaded_run_id    VARCHAR,
    _loaded_at        TIMESTAMP
);

CREATE TABLE IF NOT EXISTS etl_quarantine_log (
    run_id          VARCHAR,
    source_file     VARCHAR,
    record_key      VARCHAR,
    row_num         INTEGER,
    failure_reason  VARCHAR,
    raw_record      VARCHAR,               -- the row exactly as received, as JSON (restricted: may hold PII)
    quarantined_at  TIMESTAMP
);
