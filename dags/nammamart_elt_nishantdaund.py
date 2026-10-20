"""
dags/nammamart_elt_nishantdaund.py
----------------------------------
NammaMart ELT pipeline: Extract -> Load raw -> Transform in the warehouse with SQL.
Every source row lands untouched in raw_*; staging and marts are SQL files in
include/sql/elt/, one Airflow task per table.

  start_run
     |-> load_raw_stores ----> stg_stores ----.
     |-> load_raw_products --> stg_products --+--> stg_orders (MERGE) --> stg_payments --+--> mart_daily_store_kpis
     |-> load_raw_customers -> stg_customers -'         ^                    ^          +--> mart_category_margin
     |-> load_raw_orders -------------------------------|                    |          +--> mart_delivery_sla
     |-> load_raw_order_increments ---------------------'                    |          +--> mart_payment_reconciliation
     '-> load_raw_payments --------------------------------------------------'          '--> mart_top_customers
                                                                     record_run_audit (all_done)

Layers: raw_*  (text, append-only, _loaded_at/_source_file/_run_id)
        stg_*  (typed, standardised, deduplicated, PII masked, dq_status + failure_reason)
        mart_* (business-ready answers to the six questions)
Schedule: 09:00 Asia/Kolkata, Monday-Saturday (cron "0 9 * * 1-6"), shown as 03:30 UTC.
"""

import sys
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow.sdk import Param, dag, get_current_context, task

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "include"))

from nammamart import config, elt, runtime  # noqa: E402

RAW = ("stores", "products", "customers", "orders", "order_increments", "payments")
MASTERS = ("stores", "products", "customers")
STEPS = (["start_run"] + [f"load_raw_{t}" for t in RAW] + [f"stg_{t}" for t in MASTERS]
         + ["stg_orders", "stg_payments"] + list(elt.MARTS))

DEFAULT_ARGS = {
    "owner": config.LEARNER,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "execution_timeout": timedelta(minutes=10),
}

PARAMS = {
    "source_subdir": Param(config.DEFAULT_SOURCE_SUBDIR, type="string",
                           description="Dataset folder under include/data/ (experiments use their own folder)."),
    "include_increments": Param(True, type="boolean",
                                description="Also load orders_YYYY-MM-DD_increment.csv files found in the folder."),
    "as_of": Param(None, type=["null", "string"], pattern=r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})?$",
                   description="Replay as if run at this IST time (YYYY-MM-DD HH:MM:SS). Blank = the run's own time."),
}


@dag(
    dag_id=config.ELT_DAG_ID,
    description="NammaMart ELT: raw kept forever, SQL staging and marts by 09:00 IST",
    schedule="0 9 * * 1-6",
    start_date=pendulum.datetime(2026, 10, 1, tz=config.TZ),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=25),
    default_args=DEFAULT_ARGS,
    params=PARAMS,
    tags=["nammamart", "elt", "fdaie-module2", config.LEARNER],
    doc_md=__doc__,
)
def nammamart_elt_nishantdaund():

    @task
    def start_run() -> dict:
        return runtime.start_run(get_current_context())

    @task
    def load_raw(table: str, run: dict) -> dict:
        return runtime.fail_fast_on_contract(elt.load_raw, table, run["run_id"], run["source_subdir"],
                                             run["include_increments"])

    @task
    def stage(table: str, run: dict) -> dict:
        return elt.stage_table(table, run["run_id"], runtime.run_ts(run), run["source_subdir"])

    @task
    def build_mart(name: str, run: dict) -> dict:
        return elt.build_mart(name, run["run_id"], runtime.run_ts(run), run["source_subdir"])

    @task(trigger_rule="all_done", retries=0, on_failure_callback=runtime.notify_failure)
    def record_run_audit() -> dict:
        context = get_current_context()
        ti = context["ti"]
        loaded = [ti.xcom_pull(task_ids=f"load_raw_{t}") for t in RAW]
        orders = ti.xcom_pull(task_ids="stg_orders") or {}
        counts = {
            "rows_extracted": sum(item["rows"] for item in loaded if item),
            "rows_quarantined": elt.quarantine_count(context["dag_run"].run_id)["total"],
            "rows_loaded": orders.get("rows_merged"),
            "rows_inserted": orders.get("rows_inserted"),
            "rows_updated": orders.get("rows_updated"),
        }
        return runtime.finalise_run(context, config.ELT_DAG_ID, STEPS, counts)

    run = start_run()
    raw = {t: load_raw.override(task_id=f"load_raw_{t}")(t, run) for t in RAW}
    staged = {}
    for table in MASTERS:
        staged[table] = stage.override(task_id=f"stg_{table}")(table, run)
        raw[table] >> staged[table]
    staged["orders"] = stage.override(task_id="stg_orders")("orders", run)
    [raw["orders"], raw["order_increments"], *(staged[t] for t in MASTERS)] >> staged["orders"]
    staged["payments"] = stage.override(task_id="stg_payments")("payments", run)
    [raw["payments"], staged["orders"]] >> staged["payments"]
    marts = [build_mart.override(task_id=name)(name, run) for name in elt.MARTS]
    staged["payments"] >> marts

    [run, *raw.values(), *staged.values(), *marts] >> record_run_audit()


nammamart_elt_nishantdaund()
