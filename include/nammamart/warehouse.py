"""
include/nammamart/warehouse.py
------------------------------
DuckDB access shared by both pipelines: a lock-tolerant connection, the audit
tables (dq_results, pipeline_runs, dq_check_catalog) and their writers.

DuckDB allows one writing process at a time. Both DAGs run at 09:00 IST and
Airflow runs each task in its own process, so connect() retries while another
task holds the file lock instead of failing the run.
"""

import time
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path

import duckdb
import pandas as pd
import pendulum

from nammamart import catalog, config

LOCK_WAIT_SECONDS = 180
LOCK_POLL_SECONDS = 2


def now_ist() -> datetime:
    return pendulum.now(config.TZ).naive()


@contextmanager
def connect(db_path: Path | None = None):
    path = Path(db_path or config.WAREHOUSE_DB)
    path.parent.mkdir(parents=True, exist_ok=True)
    deadline = time.monotonic() + LOCK_WAIT_SECONDS
    while True:
        try:
            con = duckdb.connect(str(path))
            break
        except duckdb.IOException as exc:
            if "lock" not in str(exc).lower() or time.monotonic() > deadline:
                raise
            print(f"[warehouse] {path.name} is locked by another task; retrying in {LOCK_POLL_SECONDS}s")
            time.sleep(LOCK_POLL_SECONDS)
    try:
        yield con
    finally:
        con.close()


def run_sql_file(con, path: Path) -> None:
    con.execute(path.read_text(encoding="utf-8"))


def ensure_audit_tables(con) -> None:
    run_sql_file(con, config.SQL_DIR / "audit_tables.sql")
    catalog_rows = pd.DataFrame([
        {"check_name": s.name, "table_name": s.table, "dimension": s.dimension, "severity": s.severity,
         "column_name": s.column, "description": s.description, "level": s.level}
        for s in catalog.CHECKS
    ])
    con.register("catalog_rows", catalog_rows)
    con.execute("CREATE OR REPLACE TABLE dq_check_catalog AS SELECT * FROM catalog_rows")
    con.unregister("catalog_rows")


def write_dq_results(con, run_id: str, dag_id: str, results: list[dict]) -> None:
    """Replace this run's rows for these checks (a cleared task re-run must not double up)."""
    if not results:
        return
    frame = pd.DataFrame(results).assign(run_id=run_id, dag_id=dag_id, checked_at=now_ist())
    con.register("dq_new", frame)
    con.execute("""
        DELETE FROM dq_results
        WHERE run_id = ? AND dag_id = ? AND check_name IN (SELECT check_name FROM dq_new)
    """, [run_id, dag_id])
    con.execute("""
        INSERT INTO dq_results
        SELECT run_id, dag_id, check_name, dimension, severity, rows_checked, rows_failed, status, checked_at
        FROM dq_new
    """)
    con.unregister("dq_new")


def previous_order_rows(con, dag_id: str, run_id: str) -> int | None:
    row = con.execute("""
        SELECT rows_checked FROM dq_results
        WHERE dag_id = ? AND run_id <> ? AND check_name = 'orders.volume_vs_last_run'
        ORDER BY checked_at DESC LIMIT 1
    """, [dag_id, run_id]).fetchone()
    return int(row[0]) if row else None


def write_pipeline_run(con, record: dict) -> None:
    columns = ["run_id", "dag_id", "start_time", "end_time", "rows_extracted", "rows_loaded",
               "rows_quarantined", "status", "rows_inserted", "rows_updated", "failed_step"]
    values = [record.get(col) for col in columns]
    con.execute("DELETE FROM pipeline_runs WHERE run_id = ? AND dag_id = ?", [record["run_id"], record["dag_id"]])
    con.execute(f"INSERT INTO pipeline_runs ({', '.join(columns)}) VALUES ({', '.join('?' * len(columns))})", values)


def summarise_run(con, dag_id: str, run_id: str) -> pd.DataFrame:
    return con.execute("""
        SELECT check_name, severity, rows_checked, rows_failed, status
        FROM dq_results WHERE dag_id = ? AND run_id = ? AND status <> 'PASS'
        ORDER BY severity, check_name
    """, [dag_id, run_id]).df()
