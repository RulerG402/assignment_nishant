"""
scripts/business_answers.py
---------------------------
Answers the six business questions from BOTH pipelines' final tables. Each
answer is one simple query; the SQL is printed with its result so it can be
pasted into SUBMISSION.md.

    python scripts/business_answers.py [path/to/warehouse.duckdb] [run_id]
"""

import sys
from pathlib import Path

import duckdb

PROJECT = Path(__file__).resolve().parents[1]
DEFAULT_DB = PROJECT / "include" / "warehouse_nishantdaund.duckdb"

QUESTIONS = [
    ("Q1 Daily net revenue by zone and store (delivered, after discount)",
     "SELECT order_date, zone, store_id, store_name, net_revenue FROM {t} ORDER BY order_date, store_id LIMIT 5",
     {"ETL": "etl_daily_zone_revenue", "ELT": "mart_daily_store_kpis"}),
    ("Q1b September net revenue by zone",
     "SELECT zone, sum(net_revenue) AS net_revenue FROM {t} WHERE order_date < DATE '2026-10-01' "
     "GROUP BY zone ORDER BY zone",
     {"ETL": "etl_daily_zone_revenue", "ELT": "mart_daily_store_kpis"}),
    ("Q2 Revenue and gross margin by category",
     "SELECT category, net_revenue, gross_margin, margin_pct FROM {t} ORDER BY net_revenue DESC",
     {"ETL": "etl_category_margin", "ELT": "mart_category_margin"}),
    ("Q3 Average delivery time and % within 15 minutes per store",
     "SELECT store_id, avg_delivery_minutes, pct_within_15_min FROM {t} ORDER BY store_id",
     {"ELT": "mart_delivery_sla"}),
    ("Q4a Revenue by payment mode",
     "SELECT payment_mode, sum(net_revenue) AS net_revenue FROM {t} GROUP BY 1 ORDER BY 2 DESC",
     {"ELT": "mart_order_facts"}),
    ("Q4b Amount that does not reconcile with the payment gateway",
     "SELECT mismatch_reason, count(*) AS rows, sum(unreconciled_amount) AS amount FROM {t} GROUP BY 1 "
     "UNION ALL SELECT 'TOTAL', count(*), sum(unreconciled_amount) FROM {t} ORDER BY 1",
     {"ETL": "etl_payment_reconciliation", "ELT": "mart_payment_reconciliation"}),
    ("Q5 Top 10 customers by spend (masked)",
     "SELECT spend_rank, customer_id, customer_name_masked, email_masked, phone_masked, loyalty_tier, net_spend "
     "FROM {t} WHERE spend_rank <= 10 ORDER BY spend_rank",
     {"ELT": "mart_top_customers"}),
    ("Q6 Rows rejected in the run, by file and reason",
     "SELECT source_file, failure_reason, count(*) AS rows FROM {t} WHERE run_id = '{run}' "
     "GROUP BY ALL ORDER BY 3 DESC LIMIT 8",
     {"ETL": "etl_quarantine_log", "ELT": "stg_dq_failures"}),
]


def main() -> None:
    db = Path(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_DB
    run_id = sys.argv[2] if len(sys.argv) > 2 else "final_canonical"
    con = duckdb.connect(str(db), read_only=True)
    for title, template, tables in QUESTIONS:
        for pipeline, table in tables.items():
            sql = template.format(t=table, run=run_id)
            print(f"\n### {title} [{pipeline}]\n{sql}")
            print(con.execute(sql).df().to_string(index=False))


if __name__ == "__main__":
    main()
