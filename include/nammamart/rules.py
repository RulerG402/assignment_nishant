"""
include/nammamart/rules.py
--------------------------
Python implementation of every row-level and table-level check in catalog.py,
used by the ETL pipeline (and the profiling script). Each evaluate_* function
takes raw text rows and returns an Evaluation: the same rows annotated with
dq_status / failure_reason / dq_warnings, the per-check masks, and one
dq_results row per check. Inputs are never modified.

Masks are True where the row FAILS the check.
"""

from dataclasses import dataclass, field
from datetime import datetime, timedelta

import pandas as pd

from nammamart import catalog, config
from nammamart import standardise as std

EMAIL_RE = r"^[^@\s]+@[^@\s]+\.[^@\s]+$"


@dataclass(frozen=True)
class RunContext:
    run_ts: datetime                      # naive Asia/Kolkata wall-clock time
    previous_order_rows: int | None = None


@dataclass(frozen=True)
class Evaluation:
    table: str
    frame: pd.DataFrame                   # raw columns + dq_status, failure_reason, dq_warnings
    masks: pd.DataFrame                   # one boolean column per check
    results: list[dict] = field(default_factory=list)

    @property
    def passed(self) -> pd.DataFrame:
        return self.frame[self.frame["dq_status"] != "FAILED"]

    @property
    def failed(self) -> pd.DataFrame:
        return self.frame[self.frame["dq_status"] == "FAILED"]


@dataclass(frozen=True)
class MasterRefs:
    """What the orders checks need to know about the three master files."""
    all_ids: dict[str, set]
    clean_ids: dict[str, set]
    product_mrp: dict[str, float]


# ── generic helpers ─────────────────────────────────────────────────────────

def _fails(pass_condition: pd.Series) -> pd.Series:
    """A missing (NA) outcome counts as a failure, mirroring SQL's NOT coalesce(cond, false)."""
    return ~pass_condition.astype("boolean").fillna(False).astype(bool)


def _later_duplicate(keys: pd.DataFrame | pd.Series, order: pd.Series) -> pd.Series:
    """True for the 2nd+ occurrence of a non-null key, in `order` sequence."""
    frame = keys.to_frame() if isinstance(keys, pd.Series) else keys
    ranked = (frame.assign(_order=order.astype("int64").values)
              .sort_values("_order", kind="stable").drop(columns="_order"))
    dup = ranked.duplicated(keep="first") & ranked.notna().all(axis=1)
    return dup.reindex(frame.index)


def _status(severity: str, failed: int) -> str:
    if failed == 0:
        return "PASS"
    return "FAIL" if severity == catalog.CRITICAL else "WARN"


def result_row(spec: catalog.CheckSpec, rows_checked: int, rows_failed: int) -> dict:
    return {"check_name": spec.name, "dimension": spec.dimension, "severity": spec.severity,
            "rows_checked": int(rows_checked), "rows_failed": int(rows_failed),
            "status": _status(spec.severity, rows_failed)}


def _join_names(masks: pd.DataFrame, names: list[str]) -> pd.Series:
    text = pd.Series("", index=masks.index, dtype="object")
    for name in names:
        text = text.where(~masks[name], text + name + "; ")
    return text.str.rstrip("; ")


def annotate(table: str, raw: pd.DataFrame, failures: dict[str, pd.Series]) -> Evaluation:
    specs = catalog.for_table(table)
    expected = [s.name for s in specs]
    if set(failures) != set(expected):
        raise ValueError(f"{table}: checks implemented {sorted(failures)} != catalogue {sorted(expected)}")
    masks = pd.DataFrame({name: failures[name].astype(bool) for name in expected}, index=raw.index)
    critical = [s.name for s in specs if s.severity == catalog.CRITICAL]
    warning = [s.name for s in specs if s.severity == catalog.WARNING]
    reason = _join_names(masks, critical)
    warnings = _join_names(masks, warning)
    status = pd.Series("PASSED", index=raw.index).mask(warnings != "", "WARNING").mask(reason != "", "FAILED")
    frame = raw.assign(dq_status=status, failure_reason=reason.where(reason != ""),
                       dq_warnings=warnings.where(warnings != ""))
    results = [result_row(s, len(raw), masks[s.name].sum()) for s in specs]
    return Evaluation(table, frame, masks, results)


def _not_future(dates: pd.Series, reference) -> pd.Series:
    return dates.isna() | (dates <= reference)


# ── master files ────────────────────────────────────────────────────────────

def evaluate_stores(raw: pd.DataFrame, ctx: RunContext) -> Evaluation:
    store_id = std.clean_text(raw["store_id"])
    zone = std.zone(raw["zone"])
    opened = std.to_date(raw["open_date"])
    capacity = std.to_number(raw["capacity_orders_per_day"])
    return annotate("stores", raw, {
        "stores.store_id_not_null": store_id.isna(),
        "stores.zone_not_null": zone.isna(),
        "stores.store_id_unique": _later_duplicate(store_id, raw["_row_num"]),
        "stores.zone_valid": zone.notna() & ~zone.isin(config.VALID_ZONES),
        "stores.open_date_valid": opened.isna(),
        "stores.pincode_6_digits": _fails(std.clean_text(raw["pincode"]).str.fullmatch(r"\d{6}")),
        "stores.capacity_positive": _fails(capacity > 0),
        "stores.open_date_not_future": ~_not_future(opened, ctx.run_ts),
    })


def evaluate_products(raw: pd.DataFrame, ctx: RunContext) -> Evaluation:
    product_id = std.clean_text(raw["product_id"])
    category = std.clean_text(raw["category"])
    mrp = std.to_number(raw["mrp"])
    cost = std.to_number(raw["cost_price"])
    is_old = std.clean_text(raw["product_name"]).str.endswith("(old)").fillna(False).astype(bool)
    # '(old)' catalogue rows lose to the current row; otherwise first in file wins.
    dedupe_order = is_old.astype(int) * 1_000_000 + raw["_row_num"].astype(int)
    perishable = std.clean_text(raw["is_perishable"]).str.lower()
    return annotate("products", raw, {
        "products.product_id_not_null": product_id.isna(),
        "products.category_not_null": category.isna(),
        "products.product_id_unique": _later_duplicate(product_id, dedupe_order),
        "products.category_valid": category.notna() & ~category.isin(config.VALID_CATEGORIES),
        "products.is_perishable_boolean": ~perishable.isin(["true", "false"]).fillna(False).astype(bool),
        "products.mrp_positive": _fails(mrp > 0),
        "products.cost_price_le_mrp": _fails(cost <= mrp),
        "products.launch_date_not_future": ~_not_future(std.to_date(raw["launch_date"]), ctx.run_ts),
    })


def evaluate_customers(raw: pd.DataFrame, ctx: RunContext) -> Evaluation:
    customer_id = std.clean_text(raw["customer_id"])
    email = std.clean_text(raw["email"])
    tier = std.loyalty_tier(raw["loyalty_tier"])
    signup = std.to_date(raw["signup_date"])
    tier_valid = tier.isin(config.VALID_LOYALTY_TIERS).fillna(False).astype(bool)
    return annotate("customers", raw, {
        "customers.customer_id_not_null": customer_id.isna(),
        "customers.city_not_null": std.clean_text(raw["city"]).isna(),
        "customers.customer_id_unique": _later_duplicate(customer_id, raw["_row_num"]),
        "customers.email_unique": _later_duplicate(email.str.lower(), raw["_row_num"]),
        "customers.email_format": _fails(email.str.fullmatch(EMAIL_RE)),
        "customers.phone_10_digits": _fails(std.clean_text(raw["phone"]).str.fullmatch(r"\d{10}")),
        "customers.pincode_6_digits": _fails(std.clean_text(raw["pincode"]).str.fullmatch(r"\d{6}")),
        "customers.signup_date_valid": signup.isna(),
        "customers.loyalty_tier_valid": ~tier_valid,
        "customers.loyalty_tier_casing": tier_valid & ~std.is_canonical(raw["loyalty_tier"], tier),
        "customers.signup_date_not_future": ~_not_future(signup, ctx.run_ts),
    })


def master_refs(stores: pd.DataFrame, products: pd.DataFrame, customers: pd.DataFrame) -> MasterRefs:
    """Build from annotated master frames (Evaluation.frame or a re-read validated CSV)."""
    def ids(table: str, frame: pd.DataFrame) -> set:
        return set(std.clean_text(frame[config.PRIMARY_KEYS[table]]).dropna())

    def passed(frame: pd.DataFrame) -> pd.DataFrame:
        return frame[frame["dq_status"] != "FAILED"]

    reasons = products["failure_reason"].fillna("").astype(str)
    winners = products[~reasons.str.contains("products.product_id_unique", regex=False)]
    mrp = dict(zip(std.clean_text(winners["product_id"]), std.to_number(winners["mrp"])))
    frames = {"stores": stores, "products": products, "customers": customers}
    return MasterRefs(
        all_ids={name: ids(name, frame) for name, frame in frames.items()},
        clean_ids={name: ids(name, passed(frame)) for name, frame in frames.items()},
        product_mrp={k: v for k, v in mrp.items() if pd.notna(k)},
    )


# ── orders ──────────────────────────────────────────────────────────────────

def typed_orders(raw: pd.DataFrame) -> pd.DataFrame:
    return pd.DataFrame({
        "order_id": std.clean_text(raw["order_id"]),
        "order_ts": std.to_timestamp(raw["order_ts"]),
        "customer_id": std.clean_text(raw["customer_id"]),
        "store_id": std.clean_text(raw["store_id"]),
        "product_id": std.clean_text(raw["product_id"]),
        "quantity": std.to_number(raw["quantity"]),
        "unit_price": std.to_number(raw["unit_price"]),
        "discount_pct": std.to_number(raw["discount_pct"]),
        "order_amount": std.to_number(raw["order_amount"]),
        "payment_mode": std.payment_mode(raw["payment_mode"]),
        "order_status": std.order_status(raw["order_status"]),
        "channel": std.clean_text(raw["channel"]),
        "delivery_minutes": std.to_number(raw["delivery_minutes"]),
    }, index=raw.index)


def _master_checks(t: pd.DataFrame, refs: MasterRefs) -> dict[str, pd.Series]:
    out = {}
    for column, master, label in (("customer_id", "customers", "customer"),
                                  ("product_id", "products", "product"),
                                  ("store_id", "stores", "store")):
        exists = t[column].isin(refs.all_ids[master]).fillna(False).astype(bool)
        clean = t[column].isin(refs.clean_ids[master]).fillna(False).astype(bool)
        out[f"orders.{column}_exists"] = ~exists
        out[f"orders.{label}_master_clean"] = exists & ~clean
    return out


def evaluate_orders(raw: pd.DataFrame, ctx: RunContext, refs: MasterRefs) -> Evaluation:
    """raw holds every orders source stacked in apply order, with _source_file/_cutoff/_row_num."""
    t = typed_orders(raw)
    expected = t["quantity"] * t["unit_price"] * (1 - t["discount_pct"] / 100)
    minutes = t["delivery_minutes"]
    status_valid = t["order_status"].isin(config.VALID_ORDER_STATUS).fillna(False).astype(bool)
    mode_valid = t["payment_mode"].isin(config.VALID_PAYMENT_MODES).fillna(False).astype(bool)
    mrp = t["product_id"].map(refs.product_mrp).astype("float64")
    reference = pd.to_datetime(raw["_cutoff"]).clip(upper=pd.Timestamp(ctx.run_ts))
    low, high = config.NORMAL_DELIVERY_RANGE
    return annotate("orders", raw, {
        "orders.order_id_not_null": t["order_id"].isna(),
        "orders.customer_id_not_null": t["customer_id"].isna(),
        "orders.delivered_has_delivery_minutes": (t["order_status"] == "DELIVERED").fillna(False) & minutes.isna(),
        "orders.order_id_unique": _later_duplicate(raw[["_source_file"]].assign(k=t["order_id"]), raw["_row_num"]),
        "orders.order_ts_valid": t["order_ts"].isna(),
        "orders.order_status_valid": ~status_valid,
        "orders.payment_mode_valid": ~mode_valid,
        "orders.discount_pct_in_range": _fails(t["discount_pct"].between(0, 100)),
        "orders.quantity_positive": _fails(t["quantity"] > 0),
        "orders.order_amount_matches_formula": _fails((expected - t["order_amount"]).abs() <= config.AMOUNT_TOLERANCE),
        "orders.delivery_minutes_le_120": minutes.notna() & (minutes > config.MAX_DELIVERY_MINUTES),
        "orders.delivery_minutes_typical_range": minutes.notna() & ~minutes.between(low, high),
        "orders.unit_price_le_mrp": mrp.notna() & (t["unit_price"] > mrp).fillna(False),
        **_master_checks(t, refs),
        "orders.order_status_casing": status_valid & ~std.is_canonical(raw["order_status"], t["order_status"]),
        "orders.payment_mode_casing": mode_valid & ~std.is_canonical(raw["payment_mode"], t["payment_mode"]),
        "orders.order_ts_not_future": t["order_ts"].notna() & (t["order_ts"] > reference),
    })


def order_reference(raw: pd.DataFrame) -> pd.DataFrame:
    """Latest version of every order id (highest priority, first row) for payment checks."""
    t = typed_orders(raw).assign(_priority=raw["_priority"].astype(int), _row_num=raw["_row_num"].astype(int))
    t = t[t["order_id"].notna()].sort_values(["_priority", "_row_num"], ascending=[False, True], kind="stable")
    return t.drop_duplicates("order_id", keep="first").set_index("order_id")


def order_table_checks(orders: Evaluation, stores_passed: pd.DataFrame, ctx: RunContext) -> list[dict]:
    """Freshness, volume and store-capacity checks, evaluated once per run."""
    loaded = typed_orders(orders.passed)
    newest = loaded["order_ts"].max()
    stale = pd.isna(newest) or (pd.Timestamp(ctx.run_ts) - newest) > timedelta(hours=config.FRESHNESS_MAX_AGE_HOURS)
    current_rows = len(orders.frame)
    dropped = (ctx.previous_order_rows is not None and ctx.previous_order_rows > 0
               and current_rows < config.VOLUME_DROP_THRESHOLD * ctx.previous_order_rows)

    capacity = dict(zip(std.clean_text(stores_passed["store_id"]),
                        std.to_number(stores_passed["capacity_orders_per_day"])))
    per_day = loaded.assign(order_date=loaded["order_ts"].dt.date).groupby(["store_id", "order_date"]).size()
    caps = per_day.index.get_level_values("store_id").map(capacity).to_numpy(dtype="float64")
    over = int(((per_day.to_numpy() > caps) & ~pd.isna(caps)).sum())

    spec = catalog.BY_NAME
    return [
        result_row(spec["orders.freshness_latest_order"], 1, int(stale)),
        result_row(spec["orders.volume_vs_last_run"], current_rows, int(dropped)),
        result_row(spec["orders.store_daily_capacity"], len(per_day), over),
    ]


# ── payments ────────────────────────────────────────────────────────────────

def evaluate_payments(raw: pd.DataFrame, ctx: RunContext, orders_ref: pd.DataFrame) -> Evaluation:
    payment_id = std.clean_text(raw["payment_id"])
    order_id = std.clean_text(raw["order_id"])
    ts = std.to_timestamp(raw["payment_ts"])
    mode = std.payment_mode(raw["payment_mode"])
    status = std.payment_status(raw["payment_status"])
    amount = std.to_number(raw["amount_paid"])
    mode_valid = mode.isin(config.VALID_PAYMENT_MODES).fillna(False).astype(bool)
    known = order_id.isin(orders_ref.index).fillna(False).astype(bool)
    order_amount = order_id.map(orders_ref["order_amount"]).astype("float64")
    order_mode = order_id.map(orders_ref["payment_mode"])
    order_ts = pd.to_datetime(order_id.map(orders_ref["order_ts"]))
    reference = min(ctx.run_ts, config.PAYMENTS_CUTOFF)
    mismatch = ((amount - order_amount).abs() > config.AMOUNT_TOLERANCE).fillna(False)
    return annotate("payments", raw, {
        "payments.payment_id_not_null": payment_id.isna(),
        "payments.gateway_ref_present": (mode != "Cash on Delivery").fillna(True).astype(bool)
                                        & std.clean_text(raw["gateway_ref"]).isna(),
        "payments.payment_id_unique": _later_duplicate(payment_id, raw["_row_num"]),
        "payments.payment_ts_valid": ts.isna(),
        "payments.payment_mode_valid": ~mode_valid,
        "payments.payment_status_valid": ~status.isin(config.VALID_PAYMENT_STATUS).fillna(False).astype(bool),
        "payments.amount_paid_positive": _fails(amount > 0),
        "payments.order_id_exists": ~known,
        "payments.success_amount_matches_order": (status == "SUCCESS").fillna(False).astype(bool) & mismatch,
        "payments.payment_mode_matches_order": known & mode_valid & order_mode.notna()
                                               & (mode != order_mode).fillna(False).astype(bool),
        "payments.payment_mode_casing": mode_valid & ~std.is_canonical(raw["payment_mode"], mode),
        "payments.payment_ts_not_future": ts.notna() & (ts > reference),
        "payments.payment_after_order": (ts < order_ts).fillna(False).astype(bool),
    })
