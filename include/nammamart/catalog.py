"""
include/nammamart/catalog.py
----------------------------
The data quality rule catalogue: one entry per check, shared by both pipelines.
ETL implements each check in Python (rules.py); ELT implements it in SQL
(include/sql/elt/stg_*.sql). tests/nammamart/test_catalog_parity.py asserts
both implementations cover exactly these names, so dq_results is comparable.

Severity:
  Critical -> the row is quarantined and never reaches reporting tables.
  Warning  -> the row loads; the failure is counted in dq_results.
level:
  row   -> evaluated per record; failing names go into failure_reason.
  table -> evaluated once per run (freshness, volume, capacity).
"""

from dataclasses import dataclass

CRITICAL = "Critical"
WARNING = "Warning"


@dataclass(frozen=True)
class CheckSpec:
    name: str
    table: str
    dimension: str
    severity: str
    column: str
    description: str
    level: str = "row"


def _c(name, dimension, severity, column, description, level="row"):
    table = name.split(".")[0]
    return CheckSpec(name, table, dimension, severity, column, description, level)


CHECKS: tuple[CheckSpec, ...] = (
    # ── stores ──────────────────────────────────────────────────────────────
    _c("stores.store_id_not_null", "Completeness", CRITICAL, "store_id", "store_id is present"),
    _c("stores.zone_not_null", "Completeness", WARNING, "zone", "zone is present (blank loads as 'Unassigned')"),
    _c("stores.store_id_unique", "Uniqueness", CRITICAL, "store_id", "one row per store_id"),
    _c("stores.zone_valid", "Validity", CRITICAL, "zone", "zone is North/South/East/West"),
    _c("stores.open_date_valid", "Validity", CRITICAL, "open_date", "open_date parses as YYYY-MM-DD"),
    _c("stores.pincode_6_digits", "Validity", WARNING, "pincode", "pincode is 6 digits"),
    _c("stores.capacity_positive", "Accuracy", WARNING, "capacity_orders_per_day", "capacity is above zero"),
    _c("stores.open_date_not_future", "Timeliness", CRITICAL, "open_date", "store did not open in the future"),
    # ── products ────────────────────────────────────────────────────────────
    _c("products.product_id_not_null", "Completeness", CRITICAL, "product_id", "product_id is present"),
    _c("products.category_not_null", "Completeness", CRITICAL, "category", "category is present"),
    _c("products.product_id_unique", "Uniqueness", CRITICAL, "product_id", "one row per product_id; '(old)' copies lose"),
    _c("products.category_valid", "Validity", CRITICAL, "category", "category is one of the 6 agreed categories"),
    _c("products.is_perishable_boolean", "Validity", WARNING, "is_perishable", "is_perishable is True/False"),
    _c("products.mrp_positive", "Accuracy", CRITICAL, "mrp", "mrp is above zero"),
    _c("products.cost_price_le_mrp", "Accuracy", CRITICAL, "cost_price", "cost_price does not exceed mrp"),
    _c("products.launch_date_not_future", "Timeliness", CRITICAL, "launch_date", "launch_date is not in the future"),
    # ── customers ───────────────────────────────────────────────────────────
    _c("customers.customer_id_not_null", "Completeness", CRITICAL, "customer_id", "customer_id is present"),
    _c("customers.city_not_null", "Completeness", WARNING, "city", "city is present"),
    _c("customers.customer_id_unique", "Uniqueness", CRITICAL, "customer_id", "one row per customer_id; first wins"),
    _c("customers.email_unique", "Uniqueness", WARNING, "email", "email not already used by an earlier row"),
    _c("customers.email_format", "Validity", WARNING, "email", "email looks like name@domain.tld"),
    _c("customers.phone_10_digits", "Validity", WARNING, "phone", "phone is exactly 10 digits"),
    _c("customers.pincode_6_digits", "Validity", WARNING, "pincode", "pincode is 6 digits"),
    _c("customers.signup_date_valid", "Validity", CRITICAL, "signup_date", "signup_date parses as YYYY-MM-DD"),
    _c("customers.loyalty_tier_valid", "Validity", CRITICAL, "loyalty_tier", "tier is Bronze/Silver/Gold/Platinum"),
    _c("customers.loyalty_tier_casing", "Consistency", WARNING, "loyalty_tier", "tier uses Title Case"),
    _c("customers.signup_date_not_future", "Timeliness", CRITICAL, "signup_date", "signup_date is not in the future"),
    # ── orders (backfill + increments) ──────────────────────────────────────
    _c("orders.order_id_not_null", "Completeness", CRITICAL, "order_id", "order_id is present"),
    _c("orders.customer_id_not_null", "Completeness", CRITICAL, "customer_id", "customer_id is present"),
    _c("orders.delivered_has_delivery_minutes", "Completeness", CRITICAL, "delivery_minutes",
       "DELIVERED orders carry delivery_minutes"),
    _c("orders.order_id_unique", "Uniqueness", CRITICAL, "order_id", "order_id appears once per source file"),
    _c("orders.order_ts_valid", "Validity", CRITICAL, "order_ts", "order_ts parses as YYYY-MM-DD HH:MM:SS"),
    _c("orders.order_status_valid", "Validity", CRITICAL, "order_status", "status is DELIVERED/CANCELLED/RETURNED"),
    _c("orders.payment_mode_valid", "Validity", CRITICAL, "payment_mode", "mode is UPI/Card/Cash on Delivery/Wallet"),
    _c("orders.discount_pct_in_range", "Validity", CRITICAL, "discount_pct", "discount_pct is between 0 and 100"),
    _c("orders.quantity_positive", "Accuracy", CRITICAL, "quantity", "quantity is above zero"),
    _c("orders.order_amount_matches_formula", "Accuracy", CRITICAL, "order_amount",
       "order_amount = quantity x unit_price x (1 - discount_pct/100)"),
    _c("orders.delivery_minutes_le_120", "Accuracy", CRITICAL, "delivery_minutes", "delivery took 120 minutes or less"),
    _c("orders.delivery_minutes_typical_range", "Accuracy", WARNING, "delivery_minutes",
       "delivery took 8-35 minutes"),
    _c("orders.unit_price_le_mrp", "Accuracy", WARNING, "unit_price", "unit_price does not exceed the product mrp"),
    _c("orders.customer_id_exists", "Consistency", CRITICAL, "customer_id", "customer_id exists in customers.csv"),
    _c("orders.product_id_exists", "Consistency", CRITICAL, "product_id", "product_id exists in products.csv"),
    _c("orders.store_id_exists", "Consistency", CRITICAL, "store_id", "store_id exists in stores.csv"),
    _c("orders.customer_master_clean", "Consistency", CRITICAL, "customer_id", "the customer row passed its checks"),
    _c("orders.product_master_clean", "Consistency", CRITICAL, "product_id", "the product row passed its checks"),
    _c("orders.store_master_clean", "Consistency", CRITICAL, "store_id", "the store row passed its checks"),
    _c("orders.order_status_casing", "Consistency", WARNING, "order_status", "status already upper case"),
    _c("orders.payment_mode_casing", "Consistency", WARNING, "payment_mode", "mode already canonical"),
    _c("orders.order_ts_not_future", "Timeliness", CRITICAL, "order_ts",
       "order_ts not after the run time or the file's export cut-off"),
    _c("orders.freshness_latest_order", "Timeliness", WARNING, "order_ts",
       "newest order is less than 24h old at run time", level="table"),
    _c("orders.volume_vs_last_run", "Completeness", WARNING, "*",
       "order rows did not drop by more than 90% versus the last run", level="table"),
    _c("orders.store_daily_capacity", "Consistency", WARNING, "store_id",
       "store-days within capacity_orders_per_day", level="table"),
    # ── payments ────────────────────────────────────────────────────────────
    _c("payments.payment_id_not_null", "Completeness", CRITICAL, "payment_id", "payment_id is present"),
    _c("payments.gateway_ref_present", "Completeness", WARNING, "gateway_ref", "non-cash payments carry a gateway_ref"),
    _c("payments.payment_id_unique", "Uniqueness", CRITICAL, "payment_id", "one row per payment_id"),
    _c("payments.payment_ts_valid", "Validity", CRITICAL, "payment_ts", "payment_ts parses as YYYY-MM-DD HH:MM:SS"),
    _c("payments.payment_mode_valid", "Validity", CRITICAL, "payment_mode", "mode is UPI/Card/Cash on Delivery/Wallet"),
    _c("payments.payment_status_valid", "Validity", CRITICAL, "payment_status", "status is SUCCESS/FAILED/REFUNDED"),
    _c("payments.amount_paid_positive", "Accuracy", CRITICAL, "amount_paid", "amount_paid is above zero"),
    _c("payments.order_id_exists", "Consistency", WARNING, "order_id", "payment points at a known order"),
    _c("payments.success_amount_matches_order", "Consistency", WARNING, "amount_paid",
       "SUCCESS amount equals the order_amount"),
    _c("payments.payment_mode_matches_order", "Consistency", WARNING, "payment_mode", "mode agrees with the order"),
    _c("payments.payment_mode_casing", "Consistency", WARNING, "payment_mode", "mode already canonical"),
    _c("payments.payment_ts_not_future", "Timeliness", CRITICAL, "payment_ts",
       "payment_ts not after the run time or the export cut-off"),
    _c("payments.payment_after_order", "Timeliness", WARNING, "payment_ts", "payment is not earlier than its order"),
)

BY_NAME = {spec.name: spec for spec in CHECKS}


def for_table(table: str, level: str = "row") -> list[CheckSpec]:
    return [spec for spec in CHECKS if spec.table == table and spec.level == level]
