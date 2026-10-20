"""
include/nammamart/masking.py
----------------------------
PII masking for reporting tables. Same rules as include/sql/elt/stg_customers.sql:

  email      pr***@example.com    first 2 chars of the local part, domain kept
  phone      98******21           first 2 + last 2 of a valid 10-digit number
  full_name  Priya H.             first name + last-name initial

Values that fail validation (e.g. 'N/A', 7-digit phones) mask to None: an
invalid identifier has no reporting value and should not leak partially.
"""

import re

import pandas as pd

from nammamart import standardise as std

_EMAIL = re.compile(r"^([^@\s]+)@([^@\s]+\.[^@\s]+)$")
_PHONE = re.compile(r"^\d{10}$")


def mask_email(value) -> str | None:
    if not isinstance(value, str):
        return None
    match = _EMAIL.match(value.strip())
    if not match:
        return None
    local, domain = match.groups()
    return f"{local[:2]}***@{domain}"


def mask_phone(value) -> str | None:
    if not isinstance(value, str) or not _PHONE.match(value.strip()):
        return None
    digits = value.strip()
    return f"{digits[:2]}******{digits[-2:]}"


def mask_name(value) -> str | None:
    if not isinstance(value, str) or not value.strip():
        return None
    parts = value.split()
    return parts[0] if len(parts) == 1 else f"{parts[0]} {parts[-1][0]}."


def _apply(series: pd.Series, fn) -> pd.Series:
    cleaned = std.clean_text(series).astype("object")
    return cleaned.map(lambda v: fn(v) if isinstance(v, str) else None).astype("object")


def mask_customers(frame: pd.DataFrame) -> pd.DataFrame:
    """Return a copy with full_name/email/phone replaced by masked columns."""
    return frame.assign(
        customer_name_masked=_apply(frame["full_name"], mask_name),
        email_masked=_apply(frame["email"], mask_email),
        phone_masked=_apply(frame["phone"], mask_phone),
    ).drop(columns=["full_name", "email", "phone"])
