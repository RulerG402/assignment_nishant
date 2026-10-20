"""
include/nammamart/sources.py
----------------------------
Reading source exports exactly as received (every column as text, blanks kept
as ''), plus the schema contract that makes a renamed column fail loudly
instead of loading wrong data silently.
"""

from pathlib import Path

import pandas as pd

from nammamart import config


class SchemaContractError(ValueError):
    """The file header does not match the agreed contract."""


def read_header(path: Path) -> list[str]:
    with open(path, encoding="utf-8-sig") as handle:
        return [col.strip() for col in handle.readline().rstrip("\r\n").split(",")]


def check_contract(path: Path, table: str) -> None:
    if not path.exists():
        raise FileNotFoundError(f"Source file missing: {path}")
    header = read_header(path)
    expected = config.CONTRACTS[table]
    if header != expected:
        missing = [c for c in expected if c not in header]
        unexpected = [c for c in header if c not in expected]
        raise SchemaContractError(
            f"{path.name}: header does not match the {table} contract. "
            f"missing={missing} unexpected={unexpected} (expected order {expected})"
        )


def read_raw(path: Path, table: str) -> pd.DataFrame:
    """All columns as text, nothing coerced; adds _source_file and 1-based _row_num."""
    check_contract(path, table)
    frame = pd.read_csv(path, dtype=str, keep_default_na=False, na_filter=False, encoding="utf-8-sig")
    return frame.assign(_source_file=path.name, _row_num=range(1, len(frame) + 1))


def read_master(folder: Path, table: str) -> pd.DataFrame:
    return read_raw(folder / f"{table}.csv", table)


def read_payments(folder: Path) -> pd.DataFrame:
    return read_raw(folder / "payments.csv", "payments")


def read_orders(folder: Path, include_increments: bool = True, include_base: bool = True) -> pd.DataFrame:
    """Backfill and/or increments stacked in apply order, tagged with priority and cut-off."""
    selected = [src for src in config.order_sources(folder, include_increments)
                if include_base or src["file"] != config.BASE_ORDERS_FILE]
    frames = [
        read_raw(folder / src["file"], "orders").assign(_priority=src["priority"], _cutoff=src["cutoff"])
        for src in selected
    ]
    if not frames:
        return pd.DataFrame(columns=config.CONTRACTS["orders"] + ["_source_file", "_row_num", "_priority", "_cutoff"])
    return pd.concat(frames, ignore_index=True)
