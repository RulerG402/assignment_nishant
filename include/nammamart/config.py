"""
include/nammamart/config.py
---------------------------
Single place for paths, file contracts and business constants shared by the
ETL and ELT pipelines. Paths resolve relative to this file so the same code
runs inside the Astro container (/usr/local/airflow/include) and locally.
"""

import os
import re
from datetime import datetime, timedelta
from pathlib import Path

LEARNER = "nishantdaund"

INCLUDE_DIR = Path(os.environ.get("NAMMAMART_INCLUDE_DIR", Path(__file__).resolve().parents[1]))
DATA_ROOT = INCLUDE_DIR / "data"
DEFAULT_SOURCE_SUBDIR = "nammamart"
SQL_DIR = INCLUDE_DIR / "sql"
WAREHOUSE_DB = Path(os.environ.get("NAMMAMART_WAREHOUSE", INCLUDE_DIR / f"warehouse_{LEARNER}.duckdb"))
QUARANTINE_DIR = Path(os.environ.get("NAMMAMART_QUARANTINE_DIR", INCLUDE_DIR / "quarantine" / LEARNER))
WORK_DIR = Path(os.environ.get("NAMMAMART_WORK_DIR", INCLUDE_DIR / "tmp" / LEARNER))

ETL_DAG_ID = f"nammamart_etl_{LEARNER}"
ELT_DAG_ID = f"nammamart_elt_{LEARNER}"

# ── File contracts: the exact header each export must have ──────────────────
CONTRACTS = {
    "stores": ["store_id", "store_name", "zone", "area", "pincode", "open_date",
               "manager_name", "capacity_orders_per_day"],
    "products": ["product_id", "product_name", "category", "sub_category", "brand",
                 "mrp", "cost_price", "unit", "is_perishable", "launch_date"],
    "customers": ["customer_id", "full_name", "email", "phone", "city", "area",
                  "pincode", "signup_date", "loyalty_tier", "is_active"],
    "orders": ["order_id", "order_ts", "customer_id", "store_id", "product_id", "quantity",
               "unit_price", "discount_pct", "order_amount", "payment_mode", "order_status",
               "channel", "delivery_minutes"],
    "payments": ["payment_id", "order_id", "payment_ts", "payment_mode", "amount_paid",
                 "payment_status", "gateway_ref"],
}
MASTER_TABLES = ("stores", "products", "customers")
PRIMARY_KEYS = {"stores": "store_id", "products": "product_id", "customers": "customer_id",
                "orders": "order_id", "payments": "payment_id"}

INCREMENT_PATTERN = re.compile(r"^orders_(\d{4}-\d{2}-\d{2})_increment\.csv$")
BASE_ORDERS_FILE = "orders.csv"
BASE_ORDERS_PRIORITY = 0

# Export cut-off of the September backfill (agreed with ops). An order stamped
# after its file's cut-off cannot be genuine, whatever day the pipeline runs.
BASE_ORDERS_CUTOFF = datetime(2026, 9, 30, 23, 59, 59)
# Payments settle within minutes of the order; allow an hour past the cut-off.
PAYMENTS_CUTOFF = BASE_ORDERS_CUTOFF + timedelta(hours=1)

# ── Business rules ──────────────────────────────────────────────────────────
VALID_ORDER_STATUS = ("DELIVERED", "CANCELLED", "RETURNED")
VALID_PAYMENT_MODES = ("UPI", "Card", "Cash on Delivery", "Wallet")
PAYMENT_MODE_ALIASES = {
    "upi": "UPI", "card": "Card", "wallet": "Wallet",
    "cash on delivery": "Cash on Delivery", "cod": "Cash on Delivery",
    "cashh": "Cash on Delivery",
}
VALID_PAYMENT_STATUS = ("SUCCESS", "FAILED", "REFUNDED")
VALID_LOYALTY_TIERS = ("Bronze", "Silver", "Gold", "Platinum")
VALID_ZONES = ("North", "South", "East", "West")
VALID_CATEGORIES = ("Fruits & Vegetables", "Dairy & Bakery", "Staples",
                    "Snacks & Beverages", "Personal Care", "Household")
UNASSIGNED_ZONE = "Unassigned"

TS_FORMAT = "%Y-%m-%d %H:%M:%S"
DATE_FORMAT = "%Y-%m-%d"
AMOUNT_TOLERANCE = 0.01
MAX_DELIVERY_MINUTES = 120
NORMAL_DELIVERY_RANGE = (8, 35)
SLA_MINUTES = 15
FRESHNESS_MAX_AGE_HOURS = 24
VOLUME_DROP_THRESHOLD = 0.10   # warn when today's rows < 10% of the last run

TZ = "Asia/Kolkata"


def source_dir(subdir: str | None = None) -> Path:
    """Resolve a dataset folder under include/data, refusing paths that escape it."""
    root = DATA_ROOT.resolve()
    candidate = (root / (subdir or DEFAULT_SOURCE_SUBDIR)).resolve()
    if root not in candidate.parents:
        raise ValueError(f"source_subdir must stay inside {DATA_ROOT}: got {subdir!r}")
    return candidate


def order_sources(folder: Path, include_increments: bool = True) -> list[dict]:
    """
    Every orders file to process, in apply order: the September backfill first,
    then each increment by date. Priority decides which version of an order wins.
    """
    sources = [{"file": BASE_ORDERS_FILE, "priority": BASE_ORDERS_PRIORITY,
                "cutoff": BASE_ORDERS_CUTOFF}]
    if not include_increments:
        return sources
    for path in sorted(folder.glob("orders_*_increment.csv")):
        match = INCREMENT_PATTERN.match(path.name)
        if match:
            day = datetime.strptime(match.group(1), DATE_FORMAT)
            sources.append({"file": path.name, "priority": int(day.strftime("%Y%m%d")),
                            "cutoff": day.replace(hour=23, minute=59, second=59)})
    return sources


def safe_run_id(run_id: str) -> str:
    """Airflow run ids contain ':' and '+', which are awkward in file names."""
    return re.sub(r"[^A-Za-z0-9_.-]", "_", run_id)


def run_work_dir(dag_id: str, run_id: str) -> Path:
    """
    Scratch folder for one DAG run. Namespaced by DAG id because scheduled runs of
    the ETL and ELT DAGs share run ids (scheduled__<ts>): one DAG's cleanup must
    never delete the other's files.
    """
    return WORK_DIR / dag_id / safe_run_id(run_id)
