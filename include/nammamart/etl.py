"""
include/nammamart/etl.py
------------------------
Steps of the ETL pipeline: Extract -> Validate -> Quarantine -> Transform ->
Load -> Reconcile -> Report. Each function is one Airflow task in
dags/nammamart_etl_nishantdaund.py and can also run without Airflow
(scripts/run_local.py). Steps hand over small dicts (paths + counts); the
data itself moves through CSV files in a per-run work folder.
"""

import json
from dataclasses import replace
from pathlib import Path

import pandas as pd

from nammamart import config, masking, reconcile, rules, sources, warehouse
from nammamart import standardise as std

DAG_ID = config.ETL_DAG_ID
VALIDATORS = {"stores": rules.evaluate_stores, "products": rules.evaluate_products,
              "customers": rules.evaluate_customers}


def run_dir(run_id: str) -> Path:
    return config.run_work_dir(DAG_ID, run_id)


def _write(frame: pd.DataFrame, path: Path) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(path, index=False)
    return str(path)


def _read(path: str) -> pd.DataFrame:
    return pd.read_csv(path, dtype=str, keep_default_na=False, na_filter=False)


def _summary(ev: rules.Evaluation, path: str) -> dict:
    counts = ev.frame["dq_status"].value_counts()
    return {"table": ev.table, "path": path, "rows": len(ev.frame), "failed": int(counts.get("FAILED", 0)),
            "warning": int(counts.get("WARNING", 0)), "passed": int(counts.get("PASSED", 0))}


def _record(run_id: str, results: list[dict]) -> None:
    with warehouse.connect() as con:
        warehouse.ensure_audit_tables(con)
        warehouse.write_dq_results(con, run_id, DAG_ID, results)


# ── Extract ─────────────────────────────────────────────────────────────────

def extract(table: str, run_id: str, source_subdir: str | None = None, include_increments: bool = True) -> dict:
    folder = config.source_dir(source_subdir)
    if table == "order_increments":
        frame = sources.read_orders(folder, include_increments=include_increments, include_base=False)
    elif table == "orders":
        frame = sources.read_orders(folder, include_increments=False)
    elif table == "payments":
        frame = sources.read_payments(folder)
    else:
        frame = sources.read_master(folder, table)
    path = _write(frame, run_dir(run_id) / f"extract_{table}.csv")
    files = sorted(frame["_source_file"].unique().tolist())
    print(f"[Extract] {table}: {len(frame)} rows from {files or 'no files'}")
    return {"table": table, "path": path, "rows": len(frame), "files": files}


# ── Validate ────────────────────────────────────────────────────────────────

def validate_master(table: str, extracted: dict, run_id: str, ctx: rules.RunContext) -> dict:
    ev = VALIDATORS[table](_read(extracted["path"]), ctx)
    _record(run_id, ev.results)
    summary = _summary(ev, _write(ev.frame, run_dir(run_id) / f"validated_{table}.csv"))
    print(f"[Validate] {table}: {summary}")
    return summary


def _stacked_orders(orders: dict, increments: dict) -> pd.DataFrame:
    return pd.concat([_read(orders["path"]), _read(increments["path"])], ignore_index=True)


def validate_orders(orders: dict, increments: dict, masters: dict, run_id: str, ctx: rules.RunContext) -> dict:
    raw = _stacked_orders(orders, increments)
    stores, products, customers = (_read(masters[t]["path"]) for t in ("stores", "products", "customers"))
    with warehouse.connect() as con:
        warehouse.ensure_audit_tables(con)
        ctx = replace(ctx, previous_order_rows=warehouse.previous_order_rows(con, DAG_ID, run_id))
    ev = rules.evaluate_orders(raw, ctx, rules.master_refs(stores, products, customers))
    table_results = rules.order_table_checks(ev, stores[stores["dq_status"] != "FAILED"], ctx)
    _record(run_id, ev.results + table_results)
    summary = _summary(ev, _write(ev.frame, run_dir(run_id) / "validated_orders.csv"))
    summary["table_checks"] = {r["check_name"]: r["status"] for r in table_results}
    print(f"[Validate] orders: {summary}")
    return summary


def validate_payments(payments: dict, orders: dict, increments: dict, run_id: str, ctx: rules.RunContext) -> dict:
    order_ref = rules.order_reference(_stacked_orders(orders, increments))
    ev = rules.evaluate_payments(_read(payments["path"]), ctx, order_ref)
    _record(run_id, ev.results)
    summary = _summary(ev, _write(ev.frame, run_dir(run_id) / "validated_payments.csv"))
    print(f"[Validate] payments: {summary}")
    return summary


# ── Quarantine ──────────────────────────────────────────────────────────────

def quarantine(validated: list[dict], run_id: str) -> dict:
    """Write failed rows, exactly as received plus failure_reason, to one CSV per source file."""
    config.QUARANTINE_DIR.mkdir(parents=True, exist_ok=True)
    counts, logs = {}, []
    for info in validated:
        frame = _read(info["path"])
        failed = frame[frame["dq_status"] == "FAILED"]
        raw_cols = config.CONTRACTS[info["table"]]
        for source_file in sorted(frame["_source_file"].unique()):
            group = failed[failed["_source_file"] == source_file]
            counts[source_file] = len(group)
            if group.empty:
                continue
            out = config.QUARANTINE_DIR / f"{Path(source_file).stem}_failed_{config.safe_run_id(run_id)}.csv"
            group[raw_cols + ["_source_file", "_row_num", "failure_reason"]].to_csv(out, index=False)
            print(f"[Quarantine] {len(group)} rows -> {out}")
        logs.append(pd.DataFrame({
            "run_id": run_id, "source_file": failed["_source_file"],
            "record_key": failed[config.PRIMARY_KEYS[info["table"]]], "row_num": failed["_row_num"].astype(int),
            "failure_reason": failed["failure_reason"],
            "raw_record": [json.dumps(row) for row in failed[raw_cols].to_dict("records")],
        }))
    log = pd.concat(logs, ignore_index=True).assign(quarantined_at=warehouse.now_ist())
    with warehouse.connect() as con:
        warehouse.run_sql_file(con, config.SQL_DIR / "etl" / "etl_tables.sql")
        con.register("q_new", log)
        con.execute("DELETE FROM etl_quarantine_log WHERE run_id = ?", [run_id])
        con.execute("INSERT INTO etl_quarantine_log BY NAME SELECT * FROM q_new")
    return {"by_file": counts, "total": int(sum(counts.values()))}


# ── Transform ───────────────────────────────────────────────────────────────

def _passed(path: str) -> pd.DataFrame:
    frame = _read(path)
    return frame[frame["dq_status"] != "FAILED"]


def _bool_text(series: pd.Series) -> pd.Series:
    return series.map({True: "true", False: "false"})


def build_dim_store(stores: pd.DataFrame) -> pd.DataFrame:
    return pd.DataFrame({
        "store_id": std.clean_text(stores["store_id"]), "store_name": std.clean_text(stores["store_name"]),
        "zone": std.zone(stores["zone"]).fillna(config.UNASSIGNED_ZONE), "area": std.clean_text(stores["area"]),
        "pincode": std.clean_text(stores["pincode"]), "open_date": std.to_date(stores["open_date"]).dt.date,
        "manager_name": std.clean_text(stores["manager_name"]),
        "capacity_orders_per_day": std.to_number(stores["capacity_orders_per_day"]).astype("Int64"),
    })


def build_dim_product(products: pd.DataFrame) -> pd.DataFrame:
    return pd.DataFrame({
        "product_id": std.clean_text(products["product_id"]), "product_name": std.clean_text(products["product_name"]),
        "category": std.clean_text(products["category"]), "sub_category": std.clean_text(products["sub_category"]),
        "brand": std.clean_text(products["brand"]), "mrp": std.to_number(products["mrp"]),
        "cost_price": std.to_number(products["cost_price"]), "unit": std.clean_text(products["unit"]),
        "is_perishable": std.clean_text(products["is_perishable"]).str.lower(),
        "launch_date": std.to_date(products["launch_date"]).dt.date,
    })


def build_dim_customer(customers: pd.DataFrame) -> pd.DataFrame:
    masked = masking.mask_customers(customers)
    return pd.DataFrame({
        "customer_id": std.clean_text(masked["customer_id"]),
        "customer_name_masked": masked["customer_name_masked"], "email_masked": masked["email_masked"],
        "phone_masked": masked["phone_masked"], "city": std.clean_text(masked["city"]),
        "area": std.clean_text(masked["area"]), "pincode": std.clean_text(masked["pincode"]),
        "signup_date": std.to_date(masked["signup_date"]).dt.date,
        "loyalty_tier": std.loyalty_tier(masked["loyalty_tier"]),
        "is_active": std.clean_text(masked["is_active"]).str.upper().map({"Y": "true", "N": "false"}),
    })


def build_fact(orders: pd.DataFrame, dim_product: pd.DataFrame) -> pd.DataFrame:
    """Latest passing version of each order, joined to product cost, with derived measures."""
    typed = rules.typed_orders(orders).assign(
        _source_file=orders["_source_file"], _source_priority=orders["_priority"].astype(int),
        _row_num=orders["_row_num"].astype(int))
    latest = (typed.sort_values(["_source_priority", "_row_num"], ascending=[False, True], kind="stable")
              .drop_duplicates("order_id", keep="first"))
    joined = latest.merge(dim_product[["product_id", "cost_price"]], on="product_id", how="inner",
                          validate="many_to_one")
    delivered = joined["order_status"] == "DELIVERED"
    net = joined["order_amount"].where(delivered, 0.0).round(2)
    cost = (joined["quantity"] * joined["cost_price"]).round(2)
    within = (joined["delivery_minutes"] <= config.SLA_MINUTES).where(delivered)
    return joined.assign(
        order_date=joined["order_ts"].dt.date,
        quantity=joined["quantity"].astype("Int64"),
        delivery_minutes=joined["delivery_minutes"].astype("Int64"),
        net_revenue=net, cost_amount=cost,
        gross_margin=(net - cost).where(delivered, 0.0).round(2),
        within_15_min=_bool_text(within),
    ).drop(columns=["cost_price", "_row_num"]).sort_values("order_id")


def transform(validated: dict, run_id: str) -> dict:
    out_dir = run_dir(run_id)
    dim_product = build_dim_product(_passed(validated["products"]["path"]))
    tables = {
        "etl_dim_store": build_dim_store(_passed(validated["stores"]["path"])),
        "etl_dim_product": dim_product,
        "etl_dim_customer": build_dim_customer(_passed(validated["customers"]["path"])),
        "etl_fact_orders": build_fact(_passed(validated["orders"]["path"]), dim_product),
    }
    result = {name: {"path": _write(frame, out_dir / f"transformed_{name}.csv"), "rows": len(frame)}
              for name, frame in tables.items()}
    print(f"[Transform] {({k: v['rows'] for k, v in result.items()})}")
    return result


# ── Load ────────────────────────────────────────────────────────────────────

FACT_HASH_COLUMNS = ["order_ts", "customer_id", "store_id", "product_id", "quantity", "unit_price",
                     "discount_pct", "order_amount", "payment_mode", "order_status", "channel", "delivery_minutes"]


def apply_scd2(con, run_id: str, run_ts) -> dict:
    """Bonus: keep loyalty-tier history in etl_dim_customer_scd2 from the current etl_dim_customer snapshot."""
    con.execute("CREATE OR REPLACE TEMP TABLE scd2_ctx AS SELECT ?::VARCHAR AS run_id, ?::TIMESTAMP AS run_ts",
                [run_id, run_ts])
    warehouse.run_sql_file(con, config.SQL_DIR / "etl" / "etl_scd2.sql")
    versions, current = con.execute(
        "SELECT count(*), count(*) FILTER (WHERE is_current) FROM etl_dim_customer_scd2").fetchone()
    return {"scd2_versions": int(versions), "scd2_current": int(current)}


def load_star_schema(transformed: dict, run_id: str) -> dict:
    """Dimensions are full snapshots (replace); the fact is merged on order_id (upsert)."""
    hash_expr = "md5(concat_ws('|', " + ", ".join(f"coalesce(CAST({c} AS VARCHAR), '')" for c in FACT_HASH_COLUMNS) + "))"
    with warehouse.connect() as con:
        warehouse.run_sql_file(con, config.SQL_DIR / "etl" / "etl_tables.sql")
        con.execute("BEGIN TRANSACTION")
        for dim in ("etl_dim_store", "etl_dim_product", "etl_dim_customer"):
            con.execute(f"DELETE FROM {dim}")
            con.execute(f"INSERT INTO {dim} BY NAME SELECT * FROM read_csv(?, header=true, all_varchar=true)",
                        [transformed[dim]["path"]])
        scd2 = apply_scd2(con, run_id, warehouse.now_ist())
        con.execute("CREATE OR REPLACE TEMP TABLE fact_src AS SELECT * FROM etl_fact_orders LIMIT 0")
        con.execute("INSERT INTO fact_src BY NAME SELECT * FROM read_csv(?, header=true, all_varchar=true)",
                    [transformed["etl_fact_orders"]["path"]])
        con.execute(f"UPDATE fact_src SET _row_hash = {hash_expr}, _loaded_run_id = ?, _loaded_at = ?",
                    [run_id, warehouse.now_ist()])
        inserted, updated = con.execute("""
            SELECT count(*) FILTER (WHERE t.order_id IS NULL),
                   count(*) FILTER (WHERE t.order_id IS NOT NULL AND s._source_priority >= t._source_priority
                                    AND s._row_hash <> t._row_hash)
            FROM fact_src s LEFT JOIN etl_fact_orders t USING (order_id)
        """).fetchone()
        con.execute("""
            MERGE INTO etl_fact_orders AS t USING fact_src AS s ON t.order_id = s.order_id
            WHEN MATCHED AND s._source_priority >= t._source_priority THEN UPDATE
            WHEN NOT MATCHED THEN INSERT
        """)
        con.execute("COMMIT")
        fact_rows = con.execute("SELECT count(*) FROM etl_fact_orders").fetchone()[0]
    result = {"rows_merged": transformed["etl_fact_orders"]["rows"], "rows_inserted": int(inserted),
              "rows_updated": int(updated), "fact_rows_after": int(fact_rows), **scd2}
    print(f"[Load] {result}")
    return result


# ── Reconcile & report ──────────────────────────────────────────────────────

def _typed_payments(frame: pd.DataFrame) -> pd.DataFrame:
    return pd.DataFrame({
        "payment_id": std.clean_text(frame["payment_id"]), "order_id": std.clean_text(frame["order_id"]),
        "payment_status": std.payment_status(frame["payment_status"]),
        "payment_mode": std.payment_mode(frame["payment_mode"]), "amount_paid": std.to_number(frame["amount_paid"]),
    })


def reconcile_payments(validated_payments: dict, validated_orders: dict, run_id: str) -> dict:
    payments = _typed_payments(_passed(validated_payments["path"]))
    known_ids = set(std.clean_text(_read(validated_orders["path"])["order_id"]).dropna())
    with warehouse.connect() as con:
        fact = con.execute("""
            SELECT order_id, order_status, CAST(order_amount AS DOUBLE) AS order_amount, payment_mode
            FROM etl_fact_orders
        """).df()
        result = reconcile.reconcile(fact, payments, known_ids | set(fact["order_id"]))
        result = result.assign(run_id=run_id, reconciled_at=warehouse.now_ist())
        con.register("recon", result)
        con.execute("CREATE OR REPLACE TABLE etl_payment_reconciliation AS SELECT * FROM recon")
    by_reason = result.groupby("mismatch_reason")["unreconciled_amount"].agg(["count", "sum"]).round(2)
    print(f"[Reconcile] {len(result)} mismatches\n{by_reason.to_string()}")
    return {"mismatches": len(result), "unreconciled_amount": float(result["unreconciled_amount"].sum().round(2))}


def build_reports(run_id: str) -> dict:
    with warehouse.connect() as con:
        warehouse.run_sql_file(con, config.SQL_DIR / "etl" / "etl_reports.sql")
        zone = con.execute("""
            SELECT zone, sum(delivered_orders) AS delivered_orders, sum(net_revenue) AS net_revenue
            FROM etl_daily_zone_revenue GROUP BY zone ORDER BY net_revenue DESC
        """).df()
        category = con.execute("SELECT * FROM etl_category_margin ORDER BY net_revenue DESC").df()
        total = con.execute("SELECT sum(net_revenue), sum(gross_margin) FROM etl_fact_orders").fetchone()
    print(f"[Report] Net revenue by zone\n{zone.to_string(index=False)}")
    print(f"[Report] Category margin\n{category.to_string(index=False)}")
    return {"net_revenue": float(total[0] or 0), "gross_margin": float(total[1] or 0)}
