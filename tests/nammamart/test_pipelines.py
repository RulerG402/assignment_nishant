"""End-to-end tests of both pipelines (no Airflow): idempotency, increment, parity, PII, audit."""

from datetime import datetime

import pytest

from nammamart import catalog, config, parity, warehouse

RUN_TS = datetime(2026, 10, 20, 9, 0)
CORRECTED = ("ORD000017", "ORD000271", "ORD000332", "ORD000337", "ORD000584",
             "ORD000708", "ORD000822", "ORD001025", "ORD001067", "ORD001990")


@pytest.fixture(scope="module")
def built(tmp_path_factory, runner):
    """Base run, identical re-run, then increment run, in one scratch warehouse."""
    root = tmp_path_factory.mktemp("wh")
    config.WAREHOUSE_DB, config.WORK_DIR, config.QUARANTINE_DIR = (
        root / "warehouse.duckdb", root / "work", root / "q")
    snapshots = {}
    for name, increments in (("base", False), ("rerun", False), ("increment", True)):
        runner.run_etl(f"t_{name}", increments, RUN_TS)
        runner.run_elt(f"t_{name}", increments, RUN_TS)
        with warehouse.connect() as con:
            snapshots[name] = {
                "fact": con.execute("SELECT count(*) FROM etl_fact_orders").fetchone()[0],
                "raw": con.execute("SELECT count(*) FROM raw_orders").fetchone()[0],
                "stg": con.execute("SELECT count(*) FROM stg_orders").fetchone()[0],
            }
    return snapshots


def q(sql, params=None):
    with warehouse.connect() as con:
        return con.execute(sql, params or []).fetchall()


def test_rerun_does_not_change_fact_rows(built):
    assert built["base"]["fact"] == built["rerun"]["fact"] == 2179


def test_rerun_grows_raw_but_not_staging(built):
    assert built["rerun"]["raw"] == 2 * built["base"]["raw"]      # raw is append-only history
    assert built["rerun"]["stg"] == built["base"]["stg"]


def test_increment_adds_orders_without_duplicates(built):
    assert built["increment"]["fact"] == 2315
    assert q("SELECT count(*) - count(DISTINCT order_id) FROM etl_fact_orders")[0][0] == 0
    assert q("SELECT count(*) - count(DISTINCT order_id) FROM stg_orders")[0][0] == 0


def test_corrected_orders_become_returned_in_both_pipelines(built):
    marks = ",".join("?" * len(CORRECTED))
    for sql in (f"SELECT order_status FROM etl_fact_orders WHERE order_id IN ({marks})",
                f"SELECT order_status FROM stg_orders WHERE dq_status <> 'FAILED' AND order_id IN ({marks})"):
        rows = q(sql, list(CORRECTED))
        assert rows and all(r[0] == "RETURNED" for r in rows)


def test_etl_and_elt_agree(built):
    with warehouse.connect() as con:
        result = parity.compare(con)
    assert result["match"].all(), result.to_string()


def test_no_failed_row_reaches_reporting_tables(built):
    assert q("SELECT count(*) FROM mart_order_facts f JOIN stg_orders s USING (order_id) "
             "WHERE s.dq_status = 'FAILED'")[0][0] == 0


def test_every_quarantined_row_has_a_reason(built):
    assert q("SELECT count(*) FROM etl_quarantine_log WHERE failure_reason IS NULL OR failure_reason = ''")[0][0] == 0
    assert q("SELECT count(*) FROM stg_dq_failures WHERE failure_reason IS NULL OR failure_reason = ''")[0][0] == 0


def test_raw_orders_keep_bad_rows_exactly_as_received(built):
    assert q("SELECT count(*) FROM raw_orders WHERE _run_id = 't_base' AND order_status = 'Pending??'")[0][0] == 3
    assert q("SELECT count(*) FROM raw_orders WHERE _run_id = 't_base' AND customer_id = ''")[0][0] == 18


def test_dq_results_cover_every_check_for_both_dags(built):
    expected = {s.name for s in catalog.CHECKS}
    for dag_id in (config.ETL_DAG_ID, config.ELT_DAG_ID):
        got = {r[0] for r in q("SELECT DISTINCT check_name FROM dq_results WHERE dag_id = ? AND run_id = 't_base'",
                               [dag_id])}
        assert got == expected, (dag_id, expected ^ got)


def test_every_dimension_has_checks_in_dq_results(built):
    dims = {r[0] for r in q("SELECT DISTINCT dimension FROM dq_results")}
    assert dims == {"Completeness", "Uniqueness", "Validity", "Accuracy", "Consistency", "Timeliness"}


def test_pii_is_masked_everywhere(built):
    assert q("SELECT count(*) FROM etl_dim_customer WHERE email_masked IS NOT NULL "
             "AND NOT regexp_full_match(email_masked, '.{2}\\*\\*\\*@.+')")[0][0] == 0
    assert q("SELECT count(*) FROM stg_customers WHERE phone_masked IS NOT NULL "
             "AND NOT regexp_full_match(phone_masked, '\\d{2}\\*{6}\\d{2}')")[0][0] == 0
    for table in ("etl_dim_customer", "stg_customers", "mart_top_customers"):
        cols = {r[0] for r in q("SELECT column_name FROM information_schema.columns WHERE table_name = ?", [table])}
        assert cols and cols.isdisjoint({"email", "phone", "full_name"}), table


def test_top_customers_has_ten_ranks(built):
    assert q("SELECT count(*) FROM mart_top_customers WHERE spend_rank <= 10")[0][0] >= 10


def test_delivery_sla_excludes_impossible_times(built):
    assert q("SELECT max(avg_delivery_minutes) FROM mart_delivery_sla")[0][0] < 40
