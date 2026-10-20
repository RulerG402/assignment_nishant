"""
scripts/run_local.py
--------------------
Run the ETL and ELT pipelines end to end WITHOUT Airflow, in the same task
order as the DAGs, then print the ETL-vs-ELT parity table.

    python scripts/run_local.py                 # base run, re-run, increment run
    python scripts/run_local.py --warehouse /tmp/wh.duckdb --scenario base

Writes to a separate local warehouse (default include/tmp/nishantdaund/local/)
so it never collides with the Airflow warehouse file.
"""

import argparse
import os
import sys
from datetime import datetime
from pathlib import Path

PROJECT = Path(__file__).resolve().parents[1]
SCENARIOS = {
    "base": [("local_base", False)],
    "full": [("local_base", False), ("local_rerun", False), ("local_increment", True)],
}


def configure(warehouse: Path) -> None:
    """Point the package at a scratch warehouse before it is imported."""
    os.environ["NAMMAMART_WAREHOUSE"] = str(warehouse)
    os.environ["NAMMAMART_WORK_DIR"] = str(warehouse.parent / "work")
    os.environ["NAMMAMART_QUARANTINE_DIR"] = str(warehouse.parent / "quarantine")
    sys.path.insert(0, str(PROJECT / "include"))


def run_etl(run_id: str, include_increments: bool, run_ts: datetime, source_subdir: str | None = None) -> dict:
    from nammamart import etl, rules

    ctx = rules.RunContext(run_ts)
    extracted = {t: etl.extract(t, run_id, source_subdir, include_increments)
                 for t in ("stores", "products", "customers", "orders", "order_increments", "payments")}
    validated = {t: etl.validate_master(t, extracted[t], run_id, ctx) for t in ("stores", "products", "customers")}
    validated["orders"] = etl.validate_orders(extracted["orders"], extracted["order_increments"], validated, run_id, ctx)
    validated["payments"] = etl.validate_payments(extracted["payments"], extracted["orders"],
                                                  extracted["order_increments"], run_id, ctx)
    quarantined = etl.quarantine(list(validated.values()), run_id)
    loaded = etl.load_star_schema(etl.transform(validated, run_id), run_id)
    etl.reconcile_payments(validated["payments"], validated["orders"], run_id)
    etl.build_reports(run_id)
    return {"quarantined": quarantined["total"], **loaded}


def run_elt(run_id: str, include_increments: bool, run_ts: datetime, source_subdir: str | None = None) -> dict:
    from nammamart import elt

    for table in ("stores", "products", "customers", "orders", "order_increments", "payments"):
        elt.load_raw(table, run_id, source_subdir, include_increments)
    staged = {t: elt.stage_table(t, run_id, run_ts, source_subdir)
              for t in ("stores", "products", "customers", "orders", "payments")}
    for mart in elt.MARTS:
        elt.build_mart(mart, run_id, run_ts, source_subdir)
    orders = staged["orders"]
    return {"quarantined": elt.quarantine_count(run_id)["total"], "rows_inserted": orders["rows_inserted"],
            "rows_updated": orders["rows_updated"], "stg_orders_rows": orders["stg_orders_rows"]}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--warehouse", type=Path,
                        default=PROJECT / "include" / "tmp" / "nishantdaund" / "local" / "warehouse.duckdb")
    parser.add_argument("--scenario", choices=sorted(SCENARIOS), default="full")
    parser.add_argument("--run-ts", default="2026-10-20 09:00:00", help="IST wall-clock time the run pretends to be")
    args = parser.parse_args()

    configure(args.warehouse.resolve())
    from nammamart import parity, warehouse

    run_ts = datetime.strptime(args.run_ts, "%Y-%m-%d %H:%M:%S")
    for run_id, increments in SCENARIOS[args.scenario]:
        print(f"\n===== {run_id} (increments={increments}) =====")
        print("ETL ->", run_etl(run_id, increments, run_ts))
        print("ELT ->", run_elt(run_id, increments, run_ts))

    with warehouse.connect() as con:
        print("\n===== ETL vs ELT parity =====")
        print(parity.compare(con).to_string(index=False))
        print(parity.reason_breakdown(con).to_string(index=False))


if __name__ == "__main__":
    main()
