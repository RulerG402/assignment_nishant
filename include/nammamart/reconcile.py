"""
include/nammamart/reconcile.py
------------------------------
Order-vs-payment reconciliation for the ETL pipeline. The ELT pipeline runs the
same rules in include/sql/elt/mart_payment_reconciliation.sql.

One output row per mismatch. unreconciled_amount is the money finance has to
chase for that row, so the column can be summed without double counting.

Order-level reasons (orders in the finance fact table):
  missing_payment             DELIVERED, no payment at all
  payment_failed              DELIVERED, only FAILED payments
  amount_mismatch             DELIVERED, SUCCESS total differs from order_amount
  multiple_success_payments   more than one SUCCESS payment (amount counted above)
  payment_mode_mismatch       a SUCCESS payment used a different mode (amount 0)
  returned_not_refunded       RETURNED, paid, but no REFUNDED payment
  payment_on_cancelled_order  CANCELLED but a SUCCESS payment exists
Payment-level reasons (payments with no order in the fact table):
  orphan_payment              order_id is in no orders export
  order_not_in_finance_tables order exists but was quarantined
"""

import pandas as pd

from nammamart import config

COLUMNS = ["order_id", "payment_id", "order_status", "payment_status", "order_amount",
           "amount_paid", "unreconciled_amount", "mismatch_reason"]


def _per_order(payments: pd.DataFrame) -> pd.DataFrame:
    success = payments["payment_status"] == "SUCCESS"
    return pd.DataFrame({
        "order_id": payments["order_id"],
        "is_success": success.astype(int),
        "is_refund": (payments["payment_status"] == "REFUNDED").astype(int),
        "is_failed": (payments["payment_status"] == "FAILED").astype(int),
        "success_amount": payments["amount_paid"].where(success, 0.0),
    }).groupby("order_id", as_index=False).agg(
        success_count=("is_success", "sum"), refund_count=("is_refund", "sum"),
        failed_count=("is_failed", "sum"), success_amount=("success_amount", "sum"),
    )


def _order_rows(orders: pd.DataFrame, payments: pd.DataFrame) -> list[pd.DataFrame]:
    agg = orders.merge(_per_order(payments), on="order_id", how="left")
    counts = ["success_count", "refund_count", "failed_count", "success_amount"]
    agg = agg.assign(**{c: agg[c].fillna(0) for c in counts})
    delivered = agg["order_status"] == "DELIVERED"
    paid = agg["success_count"] > 0
    diff = (agg["success_amount"] - agg["order_amount"]).abs().round(2)
    joined = payments.merge(orders[["order_id", "payment_mode"]], on="order_id", suffixes=("", "_order"))
    mode_mismatch_ids = set(joined[(joined["payment_status"] == "SUCCESS")
                                   & (joined["payment_mode"] != joined["payment_mode_order"])]["order_id"])
    rules = [
        ("missing_payment", delivered & ~paid & (agg["failed_count"] == 0), agg["order_amount"]),
        ("payment_failed", delivered & ~paid & (agg["failed_count"] > 0), agg["order_amount"]),
        ("amount_mismatch", delivered & paid & (diff > config.AMOUNT_TOLERANCE), diff),
        ("multiple_success_payments", agg["success_count"] > 1, None),
        ("payment_mode_mismatch", agg["order_id"].isin(mode_mismatch_ids), None),
        ("returned_not_refunded", (agg["order_status"] == "RETURNED") & paid & (agg["refund_count"] == 0),
         agg["success_amount"]),
        ("payment_on_cancelled_order", (agg["order_status"] == "CANCELLED") & paid, agg["success_amount"]),
    ]
    out = []
    for reason, mask, amount in rules:
        rows = agg[mask]
        out.append(pd.DataFrame({
            "order_id": rows["order_id"], "payment_id": None, "order_status": rows["order_status"],
            "payment_status": None, "order_amount": rows["order_amount"],
            "amount_paid": rows["success_amount"].where(rows["success_count"] > 0),
            "unreconciled_amount": 0.0 if amount is None else amount[mask],
            "mismatch_reason": reason,
        }))
    return out


def _payment_rows(payments: pd.DataFrame, fact_ids: set, known_ids: set) -> list[pd.DataFrame]:
    unmatched = payments[~payments["order_id"].isin(fact_ids)]
    owed = unmatched["amount_paid"].where(unmatched["payment_status"] == "SUCCESS", 0.0)
    reason = unmatched["order_id"].isin(known_ids).map({True: "order_not_in_finance_tables", False: "orphan_payment"})
    return [pd.DataFrame({
        "order_id": unmatched["order_id"], "payment_id": unmatched["payment_id"], "order_status": None,
        "payment_status": unmatched["payment_status"], "order_amount": None,
        "amount_paid": unmatched["amount_paid"], "unreconciled_amount": owed, "mismatch_reason": reason,
    })]


def reconcile(orders: pd.DataFrame, payments: pd.DataFrame, known_order_ids: set) -> pd.DataFrame:
    """orders: clean fact rows; payments: payments that passed quality checks (typed)."""
    frames = _order_rows(orders, payments) + _payment_rows(payments, set(orders["order_id"]), known_order_ids)
    frames = [f for f in frames if not f.empty]
    if not frames:
        return pd.DataFrame(columns=COLUMNS)
    result = pd.concat(frames, ignore_index=True)[COLUMNS]
    result = result.assign(unreconciled_amount=pd.to_numeric(result["unreconciled_amount"]).round(2))
    return result.sort_values(["mismatch_reason", "order_id", "payment_id"], na_position="first").reset_index(drop=True)
