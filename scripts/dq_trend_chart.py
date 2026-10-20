"""
scripts/dq_trend_chart.py
-------------------------
Bonus: data-quality trend chart built from dq_results.

For every complete run of each pipeline it plots (top) rows that failed a
Critical check (quarantined) and a Warning check (loaded, logged), and
(bottom) the share of the 65 checks that passed. Only runs that recorded the
full check set are drawn, so aborted runs do not distort the trend.

    pip install matplotlib
    python scripts/dq_trend_chart.py [path/to/warehouse.duckdb] [output.png]
"""

import sys
from pathlib import Path

import duckdb
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

PROJECT = Path(__file__).resolve().parents[1]
DEFAULT_DB = PROJECT / "include" / "warehouse_nishantdaund.duckdb"
DEFAULT_OUT = PROJECT / "docs" / "dq_trend.png"

RUN_SUMMARY_SQL = """
    SELECT dag_id, run_id, min(checked_at) AS checked_at,
           count(*) AS checks,
           sum(rows_failed) FILTER (WHERE severity = 'Critical') AS critical_rows_failed,
           sum(rows_failed) FILTER (WHERE severity = 'Warning')  AS warning_rows_failed,
           100.0 * count(*) FILTER (WHERE status = 'PASS') / count(*) AS pass_pct
    FROM dq_results
    GROUP BY dag_id, run_id
    HAVING count(*) = (SELECT count(*) FROM dq_check_catalog)
    ORDER BY dag_id, checked_at
"""


def short_label(run_id: str, position: int) -> str:
    name = run_id.replace("manual__", "").replace("scheduled__", "sched_")
    return f"{position + 1}: {name[:18]}"


def main() -> None:
    db = Path(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_DB
    out = Path(sys.argv[2]) if len(sys.argv) > 2 else DEFAULT_OUT
    con = duckdb.connect(str(db), read_only=True)
    runs = con.execute(RUN_SUMMARY_SQL).df().fillna(0)
    if runs.empty:
        raise SystemExit("No complete runs found in dq_results; run a DAG first.")

    dags = sorted(runs["dag_id"].unique())
    fig, axes = plt.subplots(2, len(dags), figsize=(7.5 * len(dags), 8), squeeze=False, sharey="row")
    for column, dag in enumerate(dags):
        data = runs[runs["dag_id"] == dag].reset_index(drop=True)
        x = range(len(data))
        top, bottom = axes[0][column], axes[1][column]
        top.bar(x, data["critical_rows_failed"], color="#c0392b", label="Critical (quarantined)")
        top.bar(x, data["warning_rows_failed"], bottom=data["critical_rows_failed"], color="#e0a526",
                label="Warning (loaded, logged)")
        top.set_title(dag.replace("nammamart_", "").replace("_nishantdaund", "").upper() + " pipeline")
        top.set_ylabel("rows failing a check")
        top.legend(loc="upper right", fontsize=8)
        bottom.plot(list(x), data["pass_pct"], marker="o", color="#2c7fb8")
        bottom.set_ylim(0, 100)
        bottom.set_ylabel("% of checks passing")
        bottom.set_xlabel("run (oldest to newest)")
        bottom.set_xticks(list(x))
        bottom.set_xticklabels([short_label(r, i) for i, r in enumerate(data["run_id"])], fontsize=6, rotation=90)
        top.set_xticks(list(x))
        top.set_xticklabels([])
    fig.suptitle("NammaMart data-quality trend (from dq_results)", fontsize=13)
    fig.tight_layout()
    out.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out, dpi=130)
    print(f"wrote {out} ({len(runs)} runs plotted)")


if __name__ == "__main__":
    main()
