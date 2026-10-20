"""
include/nammamart/runtime.py
----------------------------
Airflow-facing glue shared by both NammaMart DAGs: run context, the run-audit
row in pipeline_runs, and the failure alert. Only the DAG files import this.
"""

import shutil
from datetime import datetime

import pendulum
from airflow.sdk import Variable
from airflow.sdk.exceptions import AirflowFailException

from nammamart import config, sources, warehouse


def _ist(value) -> datetime | None:
    return pendulum.instance(value).in_timezone(config.TZ).naive() if value else None


def start_run(context) -> dict:
    """Pin one run timestamp and the run parameters so every task uses the same values."""
    dag_run = context["dag_run"]
    params = context["params"]
    run_moment = getattr(dag_run, "logical_date", None) or getattr(dag_run, "run_after", None) or pendulum.now("UTC")
    as_of = (params.get("as_of") or "").strip()
    run_ts = datetime.strptime(as_of, config.TS_FORMAT) if as_of else _ist(run_moment)
    run = {
        "run_id": dag_run.run_id,
        "run_ts": run_ts.strftime(config.TS_FORMAT),
        "start_time": (_ist(getattr(dag_run, "start_date", None)) or warehouse.now_ist()).strftime(config.TS_FORMAT),
        "source_subdir": params.get("source_subdir") or config.DEFAULT_SOURCE_SUBDIR,
        "include_increments": bool(params.get("include_increments", True)),
    }
    config.source_dir(run["source_subdir"])  # fail fast on a bad folder name
    print(f"[Run] {run}")
    return run


def run_ts(run: dict) -> datetime:
    return datetime.strptime(run["run_ts"], config.TS_FORMAT)


def fail_fast_on_contract(step, *args, **kwargs):
    """
    Run an extract/load step. A schema-contract break is deterministic, so retrying
    only delays the alert: fail immediately. A missing file stays retryable (it may
    simply be late).
    """
    try:
        return step(*args, **kwargs)
    except sources.SchemaContractError as exc:
        raise AirflowFailException(f"Schema contract broken, not retrying: {exc}") from exc


def finalise_run(context, dag_id: str, steps: list[str], counts: dict) -> dict:
    """
    Write one pipeline_runs row. A step with no XCom result did not finish, so the
    run is 'failed' and the first such step is reported. The audit task itself is
    then failed (AirflowFailException, no retries) so the DAG run turns red too.
    """
    ti = context["ti"]
    missing = [step for step in steps if ti.xcom_pull(task_ids=step) is None]
    run = ti.xcom_pull(task_ids="start_run") or {}
    record = {
        "run_id": context["dag_run"].run_id,
        "dag_id": dag_id,
        "start_time": run.get("start_time") or warehouse.now_ist(),
        "end_time": warehouse.now_ist(),
        "status": "failed" if missing else "success",
        "failed_step": missing[0] if missing else None,
        **counts,
    }
    with warehouse.connect() as con:
        warehouse.ensure_audit_tables(con)
        warehouse.write_pipeline_run(con, record)
        issues = warehouse.summarise_run(con, dag_id, record["run_id"])
    print(f"[Audit] {record}")
    if not issues.empty:
        print(f"[Audit] checks that did not pass:\n{issues.to_string(index=False)}")
    if missing:
        raise AirflowFailException(f"{dag_id} run {record['run_id']} failed at step '{missing[0]}'")
    shutil.rmtree(config.run_work_dir(dag_id, record["run_id"]), ignore_errors=True)
    return record


def notify_failure(context) -> None:
    """on_failure_callback: Slack if SLACK_WEBHOOK_URL is set as an Airflow Variable, else the task log."""
    from slack_notifier import send_slack_alert

    ti = context.get("ti")
    dag_run = context.get("dag_run")
    message = (f":red_circle: NammaMart pipeline failure\n"
               f"DAG: {getattr(ti, 'dag_id', '?')}  task: {getattr(ti, 'task_id', '?')}\n"
               f"run: {getattr(dag_run, 'run_id', '?')}  try: {getattr(ti, 'try_number', '?')}\n"
               f"error: {context.get('exception')}")
    send_slack_alert(message, webhook_url=Variable.get("SLACK_WEBHOOK_URL", default=None))
