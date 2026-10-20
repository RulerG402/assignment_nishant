"""
scripts/make_experiment_data.py
-------------------------------
Build the Part F observability experiment datasets as COPIES under
include/data/experiments/<name>/. The originals in include/data/nammamart/ are
never touched, so "restore the original file" is automatic: trigger the DAG
with {"source_subdir": "experiments/<name>"} and the next normal run reads the
untouched dataset again.

    python scripts/make_experiment_data.py
"""

import csv
import shutil
from datetime import datetime, timedelta
from pathlib import Path

PROJECT = Path(__file__).resolve().parents[1]
SOURCE = PROJECT / "include" / "data" / "nammamart"
TARGET = PROJECT / "include" / "data" / "experiments"
BASE_FILES = ("stores.csv", "products.csv", "customers.csv", "orders.csv", "payments.csv")
TS_FORMAT = "%Y-%m-%d %H:%M:%S"


def _copy_base(name: str, skip: tuple[str, ...] = ()) -> Path:
    folder = TARGET / name
    shutil.rmtree(folder, ignore_errors=True)
    folder.mkdir(parents=True)
    for file_name in BASE_FILES:
        if file_name not in skip:
            shutil.copy2(SOURCE / file_name, folder / file_name)
    return folder


def _read(path: Path) -> tuple[list[str], list[dict]]:
    with open(path, newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        return list(reader.fieldnames), list(reader)


def _write(path: Path, header: list[str], rows: list[dict]) -> None:
    with open(path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=header)
        writer.writeheader()
        writer.writerows(rows)


def _shift(value: str, days: int) -> str:
    try:
        return (datetime.strptime(value, TS_FORMAT) + timedelta(days=days)).strftime(TS_FORMAT)
    except ValueError:
        return value   # unparseable timestamps stay exactly as they were


def exp04_missing_products() -> None:
    _copy_base("exp04_missing_products", skip=("products.csv",))


def exp05_bad_rows() -> None:
    folder = _copy_base("exp05_bad_rows")
    header, rows = _read(folder / "orders.csv")
    good = {"order_ts": "2026-09-15 12:00:00", "customer_id": "C00005", "store_id": "S10", "product_id": "P0023",
            "quantity": "2", "unit_price": "222.0", "discount_pct": "0", "order_amount": "444.0",
            "payment_mode": "UPI", "order_status": "DELIVERED", "channel": "App", "delivery_minutes": "20"}
    bad = [
        {"order_id": "ORDX00001", "customer_id": ""},                                   # null customer
        {"order_id": "ORDX00002", "quantity": "-2", "order_amount": "-444.0"},           # negative quantity
        {"order_id": "ORDX00003", "store_id": "S42"},                                    # unknown store
        {"order_id": "ORDX00004", "order_status": "SHIPPED"},                            # invalid status
        {"order_id": "ORDX00005", "order_ts": "2027-03-15 10:00:00"},                    # future timestamp
    ]
    _write(folder / "orders.csv", header, rows + [{**good, **row} for row in bad])


def exp06_schema_change() -> None:
    folder = _copy_base("exp06_schema_change")
    header, rows = _read(folder / "orders.csv")
    renamed = ["total" if col == "order_amount" else col for col in header]
    _write(folder / "orders.csv", renamed, [{("total" if k == "order_amount" else k): v for k, v in r.items()}
                                            for r in rows])


def exp07_low_volume() -> None:
    folder = _copy_base("exp07_low_volume")
    header, rows = _read(folder / "orders.csv")
    _write(folder / "orders.csv", header, rows[:50])


def _price_drift(name: str, store_id: str, scale_amount: bool) -> None:
    folder = _copy_base(name)
    header, rows = _read(folder / "orders.csv")
    for row in rows:
        if row["store_id"] == store_id:
            row["unit_price"] = f"{float(row['unit_price']) * 100:.1f}"
            if scale_amount:
                row["order_amount"] = f"{float(row['order_amount']) * 100:.2f}"
    _write(folder / "orders.csv", header, rows)


def exp08_price_drift() -> None:
    """Exactly as the brief says: unit_price x100 for one store, order_amount untouched."""
    _price_drift("exp08_price_drift", "S05", scale_amount=False)


def exp08b_consistent_price_drift() -> None:
    """Harder variant: a paise-as-rupees bug upstream scales unit_price AND order_amount."""
    _price_drift("exp08b_consistent_price_drift", "S05", scale_amount=True)


def exp09_stale() -> None:
    """Every order and payment 30 days older, so the newest order is ~30 days old."""
    folder = _copy_base("exp09_stale")
    for file_name, column in (("orders.csv", "order_ts"), ("payments.csv", "payment_ts")):
        header, rows = _read(folder / file_name)
        _write(folder / file_name, header, [{**r, column: _shift(r[column], -30)} for r in rows])


def main() -> None:
    builders = [exp04_missing_products, exp05_bad_rows, exp06_schema_change, exp07_low_volume,
                exp08_price_drift, exp08b_consistent_price_drift, exp09_stale]
    for build in builders:
        build()
        print(f"built {TARGET / build.__name__}")


if __name__ == "__main__":
    main()
