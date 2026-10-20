"""
include/nammamart/elt.py
------------------------
Steps of the ELT pipeline: Extract -> Load raw -> Transform in DuckDB with SQL.

Python here only moves files into raw tables and runs the .sql files in
include/sql/elt/; every cleaning rule, check, merge and mart lives in SQL.
Each function is one Airflow task in dags/nammamart_elt_nishantdaund.py.
"""

from datetime import datetime
from pathlib import Path

import pandas as pd

from nammamart import config, sources, warehouse

DAG_ID = config.ELT_DAG_ID
ELT_SQL = config.SQL_DIR / "elt"
# read_csv treats this string as NULL; it never occurs, so '' stays '' (exact copy).
NEVER_NULL = "__NAMMAMART_KEEP_EMPTY__"
STAGED_RELATION = {"stores": "stg_stores", "products": "stg_products", "customers": "stg_customers",
                   "orders": "stg_orders_batch", "payments": "stg_payments"}
MARTS = ("mart_daily_store_kpis", "mart_category_margin", "mart_delivery_sla",
         "mart_payment_reconciliation", "mart_top_customers")


def _raw_ddl(table: str) -> str:
    columns = ", ".join(f"{col} VARCHAR" for col in config.CONTRACTS[table])
    return (f"CREATE TABLE IF NOT EXISTS raw_{table} ({columns}, _row_num BIGINT, "
            f"_source_file VARCHAR, _run_id VARCHAR, _loaded_at TIMESTAMP)")


def _files_for(table: str, folder: Path, include_increments: bool) -> tuple[str, list[str]]:
    if table == "order_increments":
        files = [s["file"] for s in config.order_sources(folder, include_increments)[1:]]
        return "orders", files
    if table == "orders":
        return "orders", [config.BASE_ORDERS_FILE]
    return table, [f"{table}.csv"]


# ── Load raw ────────────────────────────────────────────────────────────────

def load_raw(table: str, run_id: str, source_subdir: str | None = None, include_increments: bool = True) -> dict:
    """Append every row of the file(s), untouched, to raw_<table>. Never updates or deletes."""
    folder = config.source_dir(source_subdir)
    target, files = _files_for(table, folder, include_increments)
    for name in files:
        sources.check_contract(folder / name, target)
    loaded_at = warehouse.now_ist()
    counts = {}
    with warehouse.connect() as con:
        con.execute(_raw_ddl(target))
        for name in files:
            con.execute(f"""
                INSERT INTO raw_{target} BY NAME
                SELECT *, row_number() OVER () AS _row_num, ? AS _source_file, ? AS _run_id, ? AS _loaded_at
                FROM read_csv(?, header = true, all_varchar = true, nullstr = '{NEVER_NULL}', parallel = false)
            """, [name, run_id, loaded_at, str(folder / name)])
            counts[name] = con.execute(
                f"SELECT count(*) FROM raw_{target} WHERE _run_id = ? AND _source_file = ? AND _loaded_at = ?",
                [run_id, name, loaded_at]).fetchone()[0]
            print(f"[Load raw] {name}: {counts[name]} rows appended to raw_{target}")
        total = con.execute(f"SELECT count(*) FROM raw_{target}").fetchone()[0]
    return {"table": table, "files": counts, "rows": int(sum(counts.values())), "raw_rows_total": int(total)}


# ── SQL runner ──────────────────────────────────────────────────────────────

def _prepare(con, run_id: str, run_ts: datetime, source_subdir: str | None) -> None:
    """Per-connection context the .sql files read: run_ctx, order_sources, cur_raw_* views."""
    warehouse.ensure_audit_tables(con)
    warehouse.run_sql_file(con, ELT_SQL / "elt_tables.sql")
    con.execute("""
        CREATE OR REPLACE TEMP TABLE run_ctx AS
        SELECT ?::VARCHAR AS run_id, ?::VARCHAR AS dag_id, ?::TIMESTAMP AS run_ts,
               ?::TIMESTAMP AS checked_at, ?::TIMESTAMP AS base_cutoff, ?::TIMESTAMP AS payments_cutoff
    """, [run_id, DAG_ID, run_ts, warehouse.now_ist(), config.BASE_ORDERS_CUTOFF, config.PAYMENTS_CUTOFF])
    folder = config.source_dir(source_subdir)
    sources_frame = pd.DataFrame(config.order_sources(folder, include_increments=True))
    con.register("order_sources_df", sources_frame.rename(columns={"file": "source_file"}))
    con.execute("CREATE OR REPLACE TEMP TABLE order_sources AS SELECT source_file, priority, cutoff FROM order_sources_df")
    con.unregister("order_sources_df")
    for table in config.CONTRACTS:
        con.execute(_raw_ddl(table))
        # This run's most recent load of each source file (a cleared, re-run load
        # task appends a new batch; only the latest one is staged).
        con.execute(f"""
            CREATE OR REPLACE TEMP VIEW cur_raw_{table} AS
            SELECT r.* FROM raw_{table} r
            JOIN (SELECT _source_file, max(_loaded_at) AS batch_at FROM raw_{table}
                  WHERE _run_id = (SELECT run_id FROM run_ctx) GROUP BY _source_file) b
              ON r._source_file = b._source_file AND r._loaded_at = b.batch_at
            WHERE r._run_id = (SELECT run_id FROM run_ctx)
        """)


def _record_row_checks(con, table: str) -> None:
    template = (ELT_SQL / "dq_record_rows.sql").read_text(encoding="utf-8")
    con.execute(template.replace("{table}", table).replace("{relation}", STAGED_RELATION[table]))


def stage_table(table: str, run_id: str, run_ts: datetime, source_subdir: str | None = None) -> dict:
    """Run include/sql/elt/stg_<table>.sql and record its checks in dq_results."""
    if table not in STAGED_RELATION:
        raise ValueError(f"Unknown staging table {table!r}")
    with warehouse.connect() as con:
        _prepare(con, run_id, run_ts, source_subdir)
        warehouse.run_sql_file(con, ELT_SQL / f"stg_{table}.sql")
        _record_row_checks(con, table)
        relation = STAGED_RELATION[table]
        counts = dict(con.execute(f"SELECT dq_status, count(*) FROM {relation} GROUP BY 1").fetchall())
        result = {"table": table, "rows": int(sum(counts.values())), "failed": int(counts.get("FAILED", 0)),
                  "warning": int(counts.get("WARNING", 0)), "passed": int(counts.get("PASSED", 0))}
        if table == "orders":
            warehouse.run_sql_file(con, ELT_SQL / "dq_orders_table_checks.sql")
            merged = con.execute("""
                SELECT rows_inserted, rows_updated, rows_merged FROM stg_orders_merge_log
                WHERE run_id = (SELECT run_id FROM run_ctx)
            """).fetchone() or (0, 0, 0)
            result.update(rows_inserted=int(merged[0]), rows_updated=int(merged[1]), rows_merged=int(merged[2]),
                          stg_orders_rows=int(con.execute("SELECT count(*) FROM stg_orders").fetchone()[0]))
    print(f"[Stage] {result}")
    return result


def build_mart(name: str, run_id: str, run_ts: datetime, source_subdir: str | None = None) -> dict:
    if name not in MARTS:
        raise ValueError(f"Unknown mart {name!r}")
    with warehouse.connect() as con:
        _prepare(con, run_id, run_ts, source_subdir)
        warehouse.run_sql_file(con, ELT_SQL / "mart_order_facts.sql")
        warehouse.run_sql_file(con, ELT_SQL / f"{name}.sql")
        preview = con.execute(f"SELECT * FROM {name} LIMIT 10").df()
        rows = con.execute(f"SELECT count(*) FROM {name}").fetchone()[0]
    print(f"[Mart] {name}: {rows} rows\n{preview.to_string(index=False)}")
    return {"mart": name, "rows": int(rows)}


def quarantine_count(run_id: str) -> dict:
    with warehouse.connect() as con:
        warehouse.run_sql_file(con, ELT_SQL / "elt_tables.sql")
        rows = con.execute("""
            SELECT source_file, count(*) FROM stg_dq_failures WHERE run_id = ? GROUP BY 1 ORDER BY 1
        """, [run_id]).fetchall()
    by_file = {name: int(n) for name, n in rows}
    return {"by_file": by_file, "total": int(sum(by_file.values()))}
