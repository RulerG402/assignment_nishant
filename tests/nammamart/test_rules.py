"""Rule-level tests for include/nammamart/rules.py, on synthetic rows and the real files."""

from datetime import datetime

import pandas as pd
import pytest

from nammamart import config, rules, sources

CTX = rules.RunContext(datetime(2026, 10, 20, 9, 0))
REFS = rules.MasterRefs(
    all_ids={"customers": {"C1", "C9"}, "products": {"P1", "P9"}, "stores": {"S1"}},
    clean_ids={"customers": {"C1"}, "products": {"P1"}, "stores": {"S1"}},
    product_mrp={"P1": 100.0, "P9": 50.0},
)


def order(**overrides) -> dict:
    row = {"order_id": "O1", "order_ts": "2026-09-10 10:00:00", "customer_id": "C1", "store_id": "S1",
           "product_id": "P1", "quantity": "2", "unit_price": "100.0", "discount_pct": "10",
           "order_amount": "180.0", "payment_mode": "UPI", "order_status": "DELIVERED", "channel": "App",
           "delivery_minutes": "20", "_source_file": "orders.csv", "_priority": 0,
           "_cutoff": config.BASE_ORDERS_CUTOFF}
    return {**row, **overrides}


def evaluate(*rows) -> rules.Evaluation:
    frame = pd.DataFrame(list(rows)).assign(_row_num=range(1, len(rows) + 1))
    return rules.evaluate_orders(frame, CTX, REFS)


def reasons(ev: rules.Evaluation, index: int = 0) -> str:
    return ev.frame["failure_reason"].iloc[index] or ""


class TestOrderRules:
    def test_clean_order_passes(self):
        ev = evaluate(order())
        assert ev.frame["dq_status"].iloc[0] == "PASSED"

    @pytest.mark.parametrize("overrides, expected", [
        ({"customer_id": ""}, "orders.customer_id_not_null"),
        ({"quantity": "-2", "order_amount": "-180.0"}, "orders.quantity_positive"),
        ({"store_id": "S99"}, "orders.store_id_exists"),
        ({"order_status": "SHIPPED"}, "orders.order_status_valid"),
        ({"order_ts": "2027-01-01 09:00:00"}, "orders.order_ts_not_future"),
        ({"order_ts": "31/09/2026 10:00"}, "orders.order_ts_valid"),
        ({"discount_pct": "250", "order_amount": "200.0"}, "orders.discount_pct_in_range"),
        ({"order_amount": "200.0"}, "orders.order_amount_matches_formula"),
        ({"delivery_minutes": "480"}, "orders.delivery_minutes_le_120"),
        ({"delivery_minutes": ""}, "orders.delivered_has_delivery_minutes"),
        ({"customer_id": "C9"}, "orders.customer_master_clean"),
        ({"product_id": "P9", "unit_price": "50.0", "order_amount": "90.0"}, "orders.product_master_clean"),
    ])
    def test_critical_failures_are_quarantined_with_reason(self, overrides, expected):
        ev = evaluate(order(**overrides))
        assert ev.frame["dq_status"].iloc[0] == "FAILED"
        assert expected in reasons(ev)

    def test_casing_is_a_warning_not_a_failure(self):
        ev = evaluate(order(order_status="delivered", payment_mode="upi"))
        assert ev.frame["dq_status"].iloc[0] == "WARNING"
        assert ev.frame["dq_warnings"].iloc[0] == "orders.order_status_casing; orders.payment_mode_casing"

    def test_order_after_file_cutoff_is_future_even_before_run_date(self):
        ev = evaluate(order(order_ts="2026-10-05 10:00:00"))
        assert "orders.order_ts_not_future" in reasons(ev)

    def test_increment_rows_use_their_own_cutoff(self):
        increment = order(order_ts="2026-10-01 20:00:00", _source_file="orders_2026-10-01_increment.csv",
                          _priority=20261001, _cutoff=datetime(2026, 10, 1, 23, 59, 59))
        assert evaluate(increment).frame["dq_status"].iloc[0] == "PASSED"

    def test_only_the_later_duplicate_in_a_file_fails(self):
        ev = evaluate(order(), order())
        assert ev.frame["dq_status"].tolist() == ["PASSED", "FAILED"]
        assert reasons(ev, 1) == "orders.order_id_unique"

    def test_same_order_in_backfill_and_increment_is_not_a_duplicate(self):
        correction = order(order_status="RETURNED", _source_file="orders_2026-10-01_increment.csv",
                           _priority=20261001, _cutoff=datetime(2026, 10, 1, 23, 59, 59))
        ev = evaluate(order(), correction)
        assert ev.frame["dq_status"].tolist() == ["PASSED", "PASSED"]

    def test_price_above_mrp_is_a_drift_warning(self):
        ev = evaluate(order(unit_price="10000.0", order_amount="18000.0"))
        assert "orders.unit_price_le_mrp" in ev.frame["dq_warnings"].iloc[0]

    def test_failure_reasons_follow_catalogue_order(self):
        ev = evaluate(order(customer_id="", quantity="0", order_amount="0"))
        assert reasons(ev).split("; ")[:2] == ["orders.customer_id_not_null", "orders.quantity_positive"]


class TestTableChecks:
    def test_volume_drop_over_90_percent_warns(self):
        ev = evaluate(order())
        low = rules.RunContext(CTX.run_ts, previous_order_rows=2572)
        stores = pd.DataFrame({"store_id": ["S1"], "capacity_orders_per_day": ["300"]})
        results = {r["check_name"]: r for r in rules.order_table_checks(ev, stores, low)}
        assert results["orders.volume_vs_last_run"]["status"] == "WARN"
        assert results["orders.freshness_latest_order"]["status"] == "WARN"   # Sept data, Oct run
        assert results["orders.store_daily_capacity"]["status"] == "PASS"

    def test_fresh_data_and_capacity_breach(self):
        ev = evaluate(order(order_ts="2026-09-30 20:00:00"), order(order_id="O2", order_ts="2026-09-30 21:00:00"))
        ctx = rules.RunContext(datetime(2026, 10, 1, 9, 0), previous_order_rows=2)
        stores = pd.DataFrame({"store_id": ["S1"], "capacity_orders_per_day": ["1"]})
        results = {r["check_name"]: r for r in rules.order_table_checks(ev, stores, ctx)}
        assert results["orders.freshness_latest_order"]["status"] == "PASS"
        assert results["orders.volume_vs_last_run"]["status"] == "PASS"
        assert results["orders.store_daily_capacity"]["rows_failed"] == 1


class TestRealFiles:
    """Part 0: every file has at least one issue; the pipeline finds the profiled counts."""

    @pytest.fixture(scope="class")
    def evaluated(self):
        folder = config.source_dir()
        stores = rules.evaluate_stores(sources.read_master(folder, "stores"), CTX)
        products = rules.evaluate_products(sources.read_master(folder, "products"), CTX)
        customers = rules.evaluate_customers(sources.read_master(folder, "customers"), CTX)
        refs = rules.master_refs(stores.frame, products.frame, customers.frame)
        raw_orders = sources.read_orders(folder)
        orders = rules.evaluate_orders(raw_orders, CTX, refs)
        payments = rules.evaluate_payments(sources.read_payments(folder), CTX, rules.order_reference(raw_orders))
        return {"stores": stores, "products": products, "customers": customers,
                "orders": orders, "payments": payments}

    def test_every_file_has_an_issue(self, evaluated):
        for table, ev in evaluated.items():
            assert sum(r["rows_failed"] for r in ev.results) > 0, table

    @pytest.mark.parametrize("check, failed", [
        ("products.product_id_unique", 2), ("products.cost_price_le_mrp", 5), ("customers.customer_id_unique", 5),
        ("customers.signup_date_not_future", 3), ("orders.order_id_unique", 12), ("orders.customer_id_not_null", 18),
        ("orders.store_id_exists", 5), ("orders.product_id_exists", 9), ("payments.payment_id_unique", 4),
        ("payments.order_id_exists", 8), ("stores.zone_not_null", 1),
    ])
    def test_profiled_issue_counts(self, evaluated, check, failed):
        ev = evaluated[check.split(".")[0]]
        assert {r["check_name"]: r["rows_failed"] for r in ev.results}[check] == failed

    def test_old_catalogue_rows_lose_dedupe(self, evaluated):
        failed = evaluated["products"].failed
        losers = failed[failed["failure_reason"].str.contains("product_id_unique")]
        assert losers["product_name"].str.endswith("(old)").all()
