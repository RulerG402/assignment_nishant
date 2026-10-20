"""
dags/nammamart_etl_nishantdaund.py
----------------------------------
NammaMart ETL pipeline: Extract -> Validate -> Quarantine -> Transform -> Load.
Only rows that pass every Critical check reach the finance star schema.

  start_run
     |-> extract_stores ----> validate_stores ----.
     |-> extract_products --> validate_products --+--> validate_orders --.
     |-> extract_customers -> validate_customers -'                     |
     |-> extract_orders ----------------------------------------------- +--> quarantine_failed_rows
     |-> extract_order_increments ------------------------------------- |        |
     '-> extract_payments ----------------------> validate_payments ----'        v
                                         transform -> load_star_schema -> reconcile_payments -> build_reports
                                                                                     |
                               record_run_audit (trigger_rule=all_done) <------------'

Schedule: 09:00 Asia/Kolkata, Monday-Saturday (cron "0 9 * * 1-6"), shown as 03:30 UTC.
Helper code: include/nammamart/ (rules, masking, reconcile, warehouse).
Warehouse:   include/warehouse_nishantdaund.duckdb
Quarantine:  include/quarantine/nishantdaund/<file>_failed_<run_id>.csv
"""

import sys
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow.sdk import Param, dag, get_current_context, task

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "include"))

from nammamart import config, etl, rules, runtime  # noqa: E402

MASTERS = ("stores", "products", "customers")
EXTRACTS = ("stores", "products", "customers", "orders", "order_increments", "payments")
STEPS = (["start_run"] + [f"extract_{t}" for t in EXTRACTS] + [f"validate_{t}" for t in MASTERS]
         + ["validate_orders", "validate_payments", "quarantine_failed_rows", "transform",
            "load_star_schema", "reconcile_payments", "build_reports"])

DEFAULT_ARGS = {
    "owner": config.LEARNER,
    "retries": 2,                               # a missing/locked file often appears within minutes
    "retry_delay": timedelta(minutes=1),
    "execution_timeout": timedelta(minutes=10),  # no single step should take longer; fail instead of hanging
}

PARAMS = {
    "source_subdir": Param(config.DEFAULT_SOURCE_SUBDIR, type="string",
                           description="Dataset folder under include/data/ (experiments use their own folder)."),
    "include_increments": Param(True, type="boolean",
                                description="Also load orders_YYYY-MM-DD_increment.csv files found in the folder."),
    "as_of": Param(None, type=["null", "string"], pattern=r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})?$",
                   description="Replay as if run at this IST time (YYYY-MM-DD HH:MM:SS). Blank = the run's own time."),
}


def _ctx(run: dict) -> rules.RunContext:
    return rules.RunContext(runtime.run_ts(run))


@dag(
    dag_id=config.ETL_DAG_ID,
    description="NammaMart ETL: validated, reconciled finance tables by 09:00 IST",
    schedule="0 9 * * 1-6",
    start_date=pendulum.datetime(2026, 10, 1, tz=config.TZ),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=25),
    default_args=DEFAULT_ARGS,
    params=PARAMS,
    tags=["nammamart", "etl", "fdaie-module2", config.LEARNER],
    doc_md=__doc__,
)
def nammamart_etl_nishantdaund():

    @task
    def start_run() -> dict:
        return runtime.start_run(get_current_context())

    @task
    def extract(table: str, run: dict) -> dict:
        return runtime.fail_fast_on_contract(etl.extract, table, run["run_id"], run["source_subdir"],
                                             run["include_increments"])

    @task
    def validate_master(table: str, extracted: dict, run: dict) -> dict:
        return etl.validate_master(table, extracted, run["run_id"], _ctx(run))

    @task
    def validate_orders(orders: dict, increments: dict, stores: dict, products: dict,
                        customers: dict, run: dict) -> dict:
        masters = {"stores": stores, "products": products, "customers": customers}
        return etl.validate_orders(orders, increments, masters, run["run_id"], _ctx(run))

    @task
    def validate_payments(payments: dict, orders: dict, increments: dict, run: dict) -> dict:
        return etl.validate_payments(payments, orders, increments, run["run_id"], _ctx(run))

    @task
    def quarantine_failed_rows(stores: dict, products: dict, customers: dict, orders: dict,
                               payments: dict, run: dict) -> dict:
        return etl.quarantine([stores, products, customers, orders, payments], run["run_id"])

    @task
    def transform(stores: dict, products: dict, customers: dict, orders: dict, run: dict) -> dict:
        validated = {"stores": stores, "products": products, "customers": customers, "orders": orders}
        return etl.transform(validated, run["run_id"])

    @task
    def load_star_schema(transformed: dict, run: dict) -> dict:
        return etl.load_star_schema(transformed, run["run_id"])

    @task
    def reconcile_payments(payments: dict, orders: dict, loaded: dict, run: dict) -> dict:
        return etl.reconcile_payments(payments, orders, run["run_id"])

    @task
    def build_reports(reconciled: dict, run: dict) -> dict:
        return etl.build_reports(run["run_id"])

    @task(trigger_rule="all_done", retries=0, on_failure_callback=runtime.notify_failure)
    def record_run_audit() -> dict:
        context = get_current_context()
        ti = context["ti"]
        extracted = [ti.xcom_pull(task_ids=f"extract_{t}") for t in EXTRACTS]
        quarantined = ti.xcom_pull(task_ids="quarantine_failed_rows") or {}
        loaded = ti.xcom_pull(task_ids="load_star_schema") or {}
        counts = {
            "rows_extracted": sum(e["rows"] for e in extracted if e),
            "rows_quarantined": quarantined.get("total"),
            "rows_loaded": loaded.get("rows_merged"),
            "rows_inserted": loaded.get("rows_inserted"),
            "rows_updated": loaded.get("rows_updated"),
        }
        return runtime.finalise_run(context, config.ETL_DAG_ID, STEPS, counts)

    run = start_run()
    extracted = {t: extract.override(task_id=f"extract_{t}")(t, run) for t in EXTRACTS}
    masters = {t: validate_master.override(task_id=f"validate_{t}")(t, extracted[t], run) for t in MASTERS}
    orders = validate_orders(extracted["orders"], extracted["order_increments"],
                             masters["stores"], masters["products"], masters["customers"], run)
    payments = validate_payments(extracted["payments"], extracted["orders"], extracted["order_increments"], run)
    quarantined = quarantine_failed_rows(masters["stores"], masters["products"], masters["customers"],
                                         orders, payments, run)
    transformed = transform(masters["stores"], masters["products"], masters["customers"], orders, run)
    quarantined >> transformed      # failed rows are always set aside before anything loads
    loaded = load_star_schema(transformed, run)
    reconciled = reconcile_payments(payments, orders, loaded, run)
    reported = build_reports(reconciled, run)

    every_step = [run, *extracted.values(), *masters.values(), orders, payments,
                  quarantined, transformed, loaded, reconciled, reported]
    every_step >> record_run_audit()


nammamart_etl_nishantdaund()
