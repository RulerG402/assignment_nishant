"""Bonus: the SCD Type 2 customer dimension keeps loyalty-tier history and is idempotent."""

from datetime import datetime

import duckdb
import pytest

from nammamart import config, etl, warehouse

T1, T2, T3 = datetime(2026, 10, 1, 9), datetime(2026, 10, 2, 9), datetime(2026, 10, 3, 9)


@pytest.fixture
def con():
    connection = duckdb.connect(":memory:")
    warehouse.run_sql_file(connection, config.SQL_DIR / "etl" / "etl_tables.sql")
    yield connection
    connection.close()


def set_customers(con, rows):
    con.execute("DELETE FROM etl_dim_customer")
    for customer_id, tier in rows:
        con.execute("INSERT INTO etl_dim_customer (customer_id, loyalty_tier, signup_date) VALUES (?, ?, DATE '2026-01-15')",
                    [customer_id, tier])


def history(con, customer_id):
    return con.execute("SELECT loyalty_tier, valid_from, valid_to, is_current FROM etl_dim_customer_scd2 "
                       "WHERE customer_id = ? ORDER BY valid_from", [customer_id]).fetchall()


def test_first_load_opens_one_current_row_from_signup_date(con):
    set_customers(con, [("C1", "Bronze"), ("C2", "Gold")])
    result = etl.apply_scd2(con, "r1", T1)
    assert result == {"scd2_versions": 2, "scd2_current": 2}
    assert history(con, "C1") == [("Bronze", datetime(2026, 1, 15), None, True)]


def test_rerun_with_same_snapshot_changes_nothing(con):
    set_customers(con, [("C1", "Bronze")])
    etl.apply_scd2(con, "r1", T1)
    again = etl.apply_scd2(con, "r1-again", T2)
    assert again == {"scd2_versions": 1, "scd2_current": 1}


def test_tier_change_closes_old_version_and_opens_new_one(con):
    set_customers(con, [("C1", "Bronze"), ("C2", "Gold")])
    etl.apply_scd2(con, "r1", T1)
    set_customers(con, [("C1", "Silver"), ("C2", "Gold")])
    result = etl.apply_scd2(con, "r2", T2)
    assert result == {"scd2_versions": 3, "scd2_current": 2}
    assert history(con, "C1") == [("Bronze", datetime(2026, 1, 15), T2, False), ("Silver", T2, None, True)]
    assert len(history(con, "C2")) == 1                       # unchanged customer untouched


def test_change_is_applied_once_even_if_the_run_repeats(con):
    set_customers(con, [("C1", "Bronze")])
    etl.apply_scd2(con, "r1", T1)
    set_customers(con, [("C1", "Gold")])
    etl.apply_scd2(con, "r2", T2)
    repeated = etl.apply_scd2(con, "r2", T3)
    assert repeated["scd2_versions"] == 2


def test_new_customer_gets_a_row_and_history_is_never_lost(con):
    set_customers(con, [("C1", "Bronze")])
    etl.apply_scd2(con, "r1", T1)
    set_customers(con, [("C1", "Silver"), ("C3", "Platinum")])
    etl.apply_scd2(con, "r2", T2)
    set_customers(con, [("C1", "Gold"), ("C3", "Platinum")])
    result = etl.apply_scd2(con, "r3", T3)
    assert [row[0] for row in history(con, "C1")] == ["Bronze", "Silver", "Gold"]
    assert result == {"scd2_versions": 4, "scd2_current": 2}
    one_current = con.execute("SELECT max(n) FROM (SELECT count(*) FILTER (WHERE is_current) n "
                              "FROM etl_dim_customer_scd2 GROUP BY customer_id)").fetchone()[0]
    assert one_current == 1
