"""
DQ AI Guard JSON reports.
"""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# Ensure the project root is on sys.path
_PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from src.utils.config_loader import load_config
from src.utils.logger import get_logger

logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# Page config – dark theme enforced by config.toml
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="DQ AI Guard | Dashboard",
    layout="wide",
    initial_sidebar_state="expanded",
    menu_items={
        "Get Help": None,
        "Report a bug": None,
        "About": "Data Quality AI Guard – Production Data Monitoring",
    },
)

st.markdown(
    """
    <style>
        header, footer {visibility: hidden;}
        div[data-testid="metric-container"] {
            background-color: #1A1C23;
            border-radius: 8px;
            padding: 0.5rem;
        }
    </style>
    """,
    unsafe_allow_html=True,
)

# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------
@st.cache_data(ttl=30)
def load_reports(results_dir: Path) -> list[dict[str, Any]]:
    """Load all JSON reports from the results directory."""
    reports = []
    if not results_dir.exists():
        return reports

    for fpath in sorted(results_dir.glob("dq_report_*.json"), reverse=True):
        try:
            with fpath.open("r", encoding="utf-8") as fh:
                report = json.load(fh)
            report["_file"] = fpath.name
            ts_str = report.get("generated_at") or report.get("validation", {}).get("run_timestamp")
            if ts_str:
                ts_str = ts_str.replace("Z", "+00:00")
                report["_timestamp"] = datetime.fromisoformat(ts_str)
            else:
                report["_timestamp"] = datetime(1970, 1, 1, tzinfo=timezone.utc)
            reports.append(report)
        except Exception as exc:
            logger.error("Skipping %s: %s", fpath, exc)
    return reports


def build_summary_df(reports: list[dict]) -> pd.DataFrame:
    """Flatten reports into a table suitable for trend views."""
    rows = []
    for r in reports:
        val = r.get("validation", {})
        ai = r.get("ai_analysis", {})
        rows.append({
            "Timestamp": r["_timestamp"],
            "Source File": val.get("source_file", "N/A"),
            "File": r["_file"],
            "Total Checks": val.get("total_checks"),
            "Passed": val.get("passed_checks"),
            "Failed": val.get("failed_checks"),
            "Overall Success": val.get("overall_success"),
            "AI Health Score": ai.get("data_health_score"),
            "AI Severity": ai.get("overall_severity", "N/A").upper(),
        })
    return pd.DataFrame(rows).sort_values("Timestamp", ascending=False)


# ---------------------------------------------------------------------------
# Main application
# ---------------------------------------------------------------------------
def main():
    st.title("DQ AI Guard – Quality Monitoring")
    st.caption("Continuous validation and AI‑powered root‑cause analysis across datasets.")

    # --- Load config (multi‑dataset aware) ---
    try:
        raw_config = load_config()
        datasets = raw_config.get("datasets", [])
        # Use results_dir from the first dataset; fallback to "results"
        results_dir = Path(datasets[0].get("results_dir", "results")) if datasets else Path("results")
    except Exception as exc:
        st.error(f"Configuration error: {exc}")
        return

    reports = load_reports(results_dir)

    if not reports:
        st.info("No reports found. Run the pipeline to populate results.")
        return

    df = build_summary_df(reports)

    # --- Compute unique source files ---
    source_files = sorted(df["Source File"].unique())
    multiple_sources = len(source_files) > 1

    # --- Sidebar filters ---
    st.sidebar.header("Filters")

    if multiple_sources:
        selected_source = st.sidebar.selectbox("Dataset", options=source_files, index=0)
        df = df[df["Source File"] == selected_source]

    time_range = st.sidebar.selectbox(
        "Period",
        ["All time", "Last 7 days", "Last 30 days"],
        index=0,
    )
    now = datetime.now(timezone.utc)
    if time_range == "Last 7 days":
        cutoff = now - pd.Timedelta(days=7)
        df = df[df["Timestamp"] >= cutoff]
    elif time_range == "Last 30 days":
        cutoff = now - pd.Timedelta(days=30)
        df = df[df["Timestamp"] >= cutoff]

    show_successful = st.sidebar.checkbox("Show successful runs", value=True)
    if not show_successful:
        df = df[df["Overall Success"] == False]

    # --- Determine latest report for the current filter ---
    if not df.empty:
        latest_file = df.iloc[0]["File"]
        latest_report = next((r for r in reports if r["_file"] == latest_file), None)
    else:
        latest_report = None

    # --- Metrics cards ---
    col1, col2, col3, col4 = st.columns(4)
    total_runs = len(df)
    failed_runs = len(df[df["Overall Success"] == False])
    latest_health = df["AI Health Score"].iloc[0] if not df.empty else None
    latest_severity = df["AI Severity"].iloc[0] if not df.empty else "N/A"

    col1.metric("Total Runs", total_runs)
    col2.metric("Failed Runs", failed_runs, delta=None)
    col3.metric("Latest Health Score", f"{latest_health}/100" if latest_health is not None else "N/A")
    col4.metric("Latest Severity", latest_severity)

    st.markdown("---")

    # --- Health score trend ---
    st.subheader("Data Health Score Over Time")
    trend_df = df.dropna(subset=["AI Health Score"]).sort_values("Timestamp")
    if not trend_df.empty:
        fig = px.line(
            trend_df,
            x="Timestamp",
            y="AI Health Score",
            markers=True,
            title="AI‑driven health score (0 = critical, 100 = clean)",
            labels={"AI Health Score": "Health Score", "Timestamp": ""},
            template="plotly_dark",
        )
        fig.add_hline(y=80, line_dash="dot", line_color="green", annotation_text="Healthy")
        fig.add_hline(y=50, line_dash="dot", line_color="orange", annotation_text="Warning")
        fig.update_layout(margin=dict(t=30, b=10))
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No health score data for the selected period.")

    # --- Latest run breakdown (uses filtered latest_report) ---
    st.subheader("Latest Validation Run")
    if latest_report:
        val = latest_report.get("validation", {})
        passed = val.get("passed_checks", 0)
        failed = val.get("failed_checks", 0)
        overall = val.get("overall_success")

        col_a, col_b = st.columns([1, 2])
        fig_pie = go.Figure(
            data=[
                go.Pie(
                    labels=["Passed", "Failed"],
                    values=[passed, failed],
                    hole=0.4,
                    marker_colors=["#00CC96", "#EF553B"],
                )
            ]
        )
        fig_pie.update_layout(margin=dict(t=0, b=0, l=0, r=0), template="plotly_dark")
        col_a.plotly_chart(fig_pie, use_container_width=True)

        col_b.write(f"**Run timestamp:** {val.get('run_timestamp', 'N/A')}")
        col_b.write(f"**Total checks:** {passed + failed}")
        col_b.write(f"**Passed:** {passed} | **Failed:** {failed}")
        status_text = "PASSED" if overall else "FAILED"
        status_color = "green" if overall else "red"
        col_b.markdown(f"**Overall status:** <span style='color:{status_color};font-weight:bold;'>{status_text}</span>", unsafe_allow_html=True)
    else:
        st.info("No runs available for the selected filters.")

    # --- Failed checks detail (uses filtered latest_report) ---
    st.subheader("Failed Checks (Latest Run)")
    if latest_report:
        failed_details = [r for r in latest_report["validation"].get("results", []) if not r.get("success")]
        if failed_details:
            fail_df = pd.DataFrame(failed_details)[
                ["check_name", "failed_count", "total_count", "percentage_failed"]
            ]
            fail_df.columns = ["Check", "Failed", "Total", "% Failed"]
            st.dataframe(fail_df, use_container_width=True)
        else:
            st.success("All checks passed in this run.")
    else:
        st.info("No data.")

    # --- AI Analysis (uses filtered latest_report) ---
    st.subheader("AI Root‑Cause Analysis (Latest Run)")
    if latest_report:
        ai = latest_report.get("ai_analysis", {})
        if ai.get("error"):
            st.warning(f"AI analysis unavailable: {ai.get('reason')}")
        else:
            st.write(f"**Summary:** {ai.get('analysis_summary', 'N/A')}")
            issues = ai.get("issues", [])
            if issues:
                for idx, issue in enumerate(issues, 1):
                    sev = issue.get("severity", "N/A").upper()
                    check = issue.get("check_name", "N/A")
                    with st.expander(f"{idx}. {sev} – {check}"):
                        st.write(f"**Issue:** {issue.get('issue_summary', 'N/A')}")
                        st.write(f"**Root cause:** {issue.get('root_cause', 'N/A')}")
                        st.write(f"**Recommended fix:** {issue.get('recommended_fix', 'N/A')}")
                        if col := issue.get("affected_column"):
                            st.write(f"**Affected column:** {col}")
                        if examples := issue.get("example_bad_values"):
                            st.write(f"**Example bad values:** {', '.join(map(str, examples))}")
            else:
                st.info("No issues reported by AI.")
    else:
        st.info("No data.")

    # --- Historical runs table ---
    st.subheader("Historical Runs")
    def color_failed(val: bool) -> str:
        return "color: red" if not val else "color: green"
    styled_df = df.style.applymap(color_failed, subset=["Overall Success"])
    st.dataframe(styled_df, use_container_width=True)

    # Download filtered data
    csv = df.to_csv(index=False).encode("utf-8")
    st.download_button(
        label="Download filtered runs as CSV",
        data=csv,
        file_name=f"dq_runs_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
        mime="text/csv",
    )


if __name__ == "__main__":
    main()