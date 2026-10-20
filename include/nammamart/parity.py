"""
include/nammamart/parity.py
---------------------------
Side-by-side comparison of the ETL and ELT outputs (acceptance: "mart totals
must match ETL totals"). Read-only; used by scripts/run_local.py and tests.
"""

import pandas as pd

PAIRS = [
    ("September net revenue",
     "SELECT sum(net_revenue) FROM etl_fact_orders WHERE order_date < DATE '2026-10-01'",
     "SELECT sum(net_revenue) FROM mart_order_facts WHERE order_date < DATE '2026-10-01'"),
    ("All-time net revenue", "SELECT sum(net_revenue) FROM etl_fact_orders",
     "SELECT sum(net_revenue) FROM mart_order_facts"),
    ("Gross margin", "SELECT sum(gross_margin) FROM etl_fact_orders",
     "SELECT sum(gross_margin) FROM mart_order_facts"),
    ("Orders in finance tables", "SELECT count(*) FROM etl_fact_orders", "SELECT count(*) FROM mart_order_facts"),
    ("Delivered orders", "SELECT count(*) FROM etl_fact_orders WHERE order_status = 'DELIVERED'",
     "SELECT count(*) FROM mart_order_facts WHERE order_status = 'DELIVERED'"),
    ("RETURNED orders", "SELECT count(*) FROM etl_fact_orders WHERE order_status = 'RETURNED'",
     "SELECT count(*) FROM mart_order_facts WHERE order_status = 'RETURNED'"),
    ("Category rows", "SELECT count(*) FROM etl_category_margin", "SELECT count(*) FROM mart_category_margin"),
    ("Reconciliation mismatches", "SELECT count(*) FROM etl_payment_reconciliation",
     "SELECT count(*) FROM mart_payment_reconciliation"),
    ("Unreconciled amount", "SELECT sum(unreconciled_amount) FROM etl_payment_reconciliation",
     "SELECT sum(unreconciled_amount) FROM mart_payment_reconciliation"),
    ("Rows quarantined (latest run)",
     "SELECT count(*) FROM etl_quarantine_log WHERE run_id = (SELECT max_by(run_id, quarantined_at) FROM etl_quarantine_log)",
     "SELECT count(*) FROM stg_dq_failures WHERE run_id = (SELECT max_by(run_id, recorded_at) FROM stg_dq_failures)"),
]


def _scalar(con, sql: str):
    value = con.execute(sql).fetchone()[0]
    return round(float(value), 2) if value is not None else None


def compare(con) -> pd.DataFrame:
    rows = []
    for metric, etl_sql, elt_sql in PAIRS:
        etl_value, elt_value = _scalar(con, etl_sql), _scalar(con, elt_sql)
        rows.append({"metric": metric, "etl": etl_value, "elt": elt_value, "match": etl_value == elt_value})
    return pd.DataFrame(rows)


def reason_breakdown(con) -> pd.DataFrame:
    """Reconciliation counts per reason, both pipelines, outer-joined."""
    return con.execute("""
        WITH e AS (SELECT mismatch_reason, count(*) AS etl_rows, round(sum(unreconciled_amount), 2) AS etl_amount
                   FROM etl_payment_reconciliation GROUP BY 1),
             l AS (SELECT mismatch_reason, count(*) AS elt_rows, round(sum(unreconciled_amount), 2) AS elt_amount
                   FROM mart_payment_reconciliation GROUP BY 1)
        SELECT * FROM e FULL OUTER JOIN l USING (mismatch_reason) ORDER BY mismatch_reason
    """).df()
