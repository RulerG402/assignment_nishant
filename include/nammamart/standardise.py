"""
include/nammamart/standardise.py
--------------------------------
Pure pandas helpers that turn raw text columns into typed, canonical values.
Every function returns a new Series; inputs are never modified. The SQL in
include/sql/elt/stg_*.sql implements the same rules for the ELT pipeline.
"""

import pandas as pd

from nammamart import config


def clean_text(series: pd.Series) -> pd.Series:
    """Trim whitespace; empty strings become missing."""
    stripped = series.astype("string").str.strip()
    return stripped.mask(stripped == "")


def to_number(series: pd.Series) -> pd.Series:
    return pd.to_numeric(clean_text(series), errors="coerce").astype("float64")


def to_timestamp(series: pd.Series) -> pd.Series:
    """Strict YYYY-MM-DD HH:MM:SS; anything else (N/A, 31/09/2026, month 13) is NaT."""
    return pd.to_datetime(clean_text(series), format=config.TS_FORMAT, errors="coerce")


def to_date(series: pd.Series) -> pd.Series:
    return pd.to_datetime(clean_text(series), format=config.DATE_FORMAT, errors="coerce")


def order_status(series: pd.Series) -> pd.Series:
    return clean_text(series).str.upper()


def payment_mode(series: pd.Series) -> pd.Series:
    """Map known spellings (upi, 'UPI ', cod, Cashh, CARD) to the canonical mode."""
    lowered = clean_text(series).str.lower()
    return lowered.map(lambda v: config.PAYMENT_MODE_ALIASES.get(v, v) if isinstance(v, str) else v)


def payment_status(series: pd.Series) -> pd.Series:
    return clean_text(series).str.upper()


def loyalty_tier(series: pd.Series) -> pd.Series:
    return clean_text(series).str.title()


def zone(series: pd.Series) -> pd.Series:
    return clean_text(series).str.title()


def is_canonical(raw: pd.Series, canonical: pd.Series) -> pd.Series:
    """True when the raw text already equals its canonical form (or both are missing)."""
    same = raw.astype("string") == canonical
    return same.fillna(False) | (canonical.isna() & clean_text(raw).isna())
