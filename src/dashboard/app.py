"""
DQ AI Guard Dashboard
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

_PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from src.utils.config_loader import load_config
from src.utils.logger import get_logger

logger = get_logger(__name__)

# ------------------------------------------------------------
# Page Config
# ------------------------------------------------------------
st.set_page_config(
    page_title="DQ AI Guard | Dashboard",
    layout="wide"
)

st.markdown("""
<style>
header, footer {visibility: hidden;}
div[data-testid="metric-container"] {
    background-color: #1A1C23;
    border-radius: 10px;
    padding: 0.8rem;
}
</style>
""", unsafe_allow_html=True)

# ------------------------------------------------------------
# Data Load
# ------------------------------------------------------------
@st.cache_data(ttl=30)
def load_reports(results_dir: Path) -> list[dict[str, Any]]:
    reports = []

    if not results_dir.exists():
        return reports

    for f in sorted(results_dir.glob("dq_report_*.json"), reverse=True):
        try:
            with f.open("r") as fh:
                r = json.load(fh)

            r["_file"] = f.name

            ts = r.get("generated_at") or r.get("validation", {}).get("run_timestamp")
            if ts:
                ts = ts.replace("Z", "+00:00")
                r["_timestamp"] = datetime.fromisoformat(ts)
            else:
                r["_timestamp"] = datetime(1970, 1, 1, tzinfo=timezone.utc)

            reports.append(r)

        except Exception as e:
            logger.error(f"Skipping {f}: {e}")

    return reports


def build_df(reports):
    rows = []
    for r in reports:
        val = r.get("validation", {})
        ai = r.get("ai_analysis", {})

        rows.append({
            "Timestamp": r["_timestamp"],
            "Dataset": val.get("source_file"),
            "File": r["_file"],
            "Passed": val.get("passed_checks"),
            "Failed": val.get("failed_checks"),
            "Success": val.get("overall_success"),
            "Health": ai.get("data_health_score"),
            "Severity": ai.get("overall_severity", "N/A").upper()
        })

    return pd.DataFrame(rows).sort_values("Timestamp", ascending=False)


# ------------------------------------------------------------
# Helper: Determine line colour from latest health score
# ------------------------------------------------------------
def _health_line_colour(df: pd.DataFrame) -> str:
    """Return a colour for the health trend line based on the latest health score."""
    if df.empty or df["Health"].isna().all():
        return "#94A3B8"           # neutral grey if no data

    latest = df["Health"].iloc[0]
    if latest < 50:
        return "#F87171"           # red
    elif latest < 80:
        return "#FBBF24"           # yellow / orange
    else:
        return "#4ADE80"           # green


# ------------------------------------------------------------
# Main
# ------------------------------------------------------------
def main():
    st.title("DQ AI Guard Dashboard")
    st.caption("Data Quality Monitoring with AI Diagnostics")

    config = load_config()
    datasets = config.get("datasets", [])
    results_dir = Path(datasets[0].get("results_dir", "results")) if datasets else Path("results")

    reports = load_reports(results_dir)

    if not reports:
        st.info("No reports available.")
        return

    df = build_df(reports)

    # --------------------------------------------------------
    # Sidebar
    # --------------------------------------------------------
    st.sidebar.header("Filters")

    if len(df["Dataset"].unique()) > 1:
        ds = st.sidebar.selectbox("Dataset", sorted(df["Dataset"].unique()))
        df = df[df["Dataset"] == ds]

    time_range = st.sidebar.selectbox("Time Range", ["All", "7D", "30D"])

    now = datetime.now(timezone.utc)

    if time_range == "7D":
        df = df[df["Timestamp"] >= now - pd.Timedelta(days=7)]
    elif time_range == "30D":
        df = df[df["Timestamp"] >= now - pd.Timedelta(days=30)]

    if not st.sidebar.checkbox("Include Successful", True):
        df = df[df["Success"] == False]

    # Latest report
    latest = None
    if not df.empty:
        latest_file = df.iloc[0]["File"]
        latest = next((r for r in reports if r["_file"] == latest_file), None)

    # --------------------------------------------------------
    # KPI
    # --------------------------------------------------------
    c1, c2, c3, c4 = st.columns(4)

    c1.metric("Total Runs", len(df))
    c2.metric("Failed Runs", len(df[df["Success"] == False]))

    health = df["Health"].iloc[0] if not df.empty else None
    sev = df["Severity"].iloc[0] if not df.empty else "N/A"

    c3.metric("Health Score", f"{health}/100" if health else "N/A")
    c4.metric("Severity", sev)

    st.markdown("---")

    # --------------------------------------------------------
    # Trends
    # --------------------------------------------------------
    st.subheader("Trends Overview")

    col1, col2 = st.columns(2)

    # --- Health trend (line chart) ---
    trend_df = df.dropna(subset=["Health"]).sort_values("Timestamp")

    if not trend_df.empty:
        line_colour = _health_line_colour(trend_df)

        fig_line = go.Figure()
        fig_line.add_trace(
            go.Scatter(
                x=trend_df["Timestamp"],
                y=trend_df["Health"],
                mode="lines+markers",
                line=dict(color=line_colour, width=2),
                marker=dict(color=line_colour, size=6),
                name="Health Score",
            )
        )
        fig_line.update_layout(
            template="plotly_dark",
            xaxis_title="",
            yaxis_title="Health Score",
            yaxis_range=[0, 105],
        )
        col1.plotly_chart(fig_line, use_container_width=True, key="health_line")
    else:
        col1.info("No health score data available.")

    # --- Pass / Fail bar chart ---
    df_bar = df.copy()

    if not df_bar.empty:
        df_bar["Date"] = pd.to_datetime(df_bar["Timestamp"]).dt.date
        df_bar["Success"] = df_bar["Success"].astype(str).str.lower().map({
            "true": True, "false": False
        })
        df_bar = df_bar[df_bar["Success"].isin([True, False])]

        agg = (
            df_bar.groupby(["Date", "Success"])
            .size()
            .unstack(fill_value=0)
            .rename(columns={True: "Passed", False: "Failed"})
            .reset_index()
        )

        fig_bar = go.Figure()

        # Always add traces but hide empty ones with a condition
        has_passed = "Passed" in agg.columns and agg["Passed"].sum() > 0
        has_failed = "Failed" in agg.columns and agg["Failed"].sum() > 0

        if has_passed:
            fig_bar.add_bar(
                x=agg["Date"], y=agg["Passed"],
                name="Passed",
                marker_color="#22C55E",        # green 
            )

        if has_failed:
            fig_bar.add_bar(
                x=agg["Date"], y=agg["Failed"],
                name="Failed",
                marker_color="#3B82F6",        # blue
            )

        fig_bar.update_layout(
            barmode="stack",
            template="plotly_dark",
            xaxis_title="Date",
            yaxis_title="Runs",
        )

        col2.plotly_chart(fig_bar, use_container_width=True, key="pass_fail_bar")
    else:
        col2.info("No run count data.")

    # --------------------------------------------------------
    # Failed Checks
    # --------------------------------------------------------
    st.subheader("Failed Checks Analysis")

    if latest:
        failures = [
            r for r in latest["validation"].get("results", [])
            if not r.get("success")
        ]

        if failures:
            fail_df = pd.DataFrame(failures)[
                ["check_name", "failed_count", "percentage_failed"]
            ]

            fail_df.columns = ["Check", "Failed", "Failure %"]
            fail_df = fail_df.sort_values("Failed", ascending=False)

            col5, col6 = st.columns(2)

            col5.dataframe(fail_df, use_container_width=True)

            fig_fail = px.bar(
                fail_df,
                x="Check", y="Failed",
                template="plotly_dark",
                color_discrete_sequence=["#3B82F6"]   # red for failures
            )
            fig_fail.update_layout(xaxis_tickangle=-30)
            col6.plotly_chart(fig_fail, use_container_width=True, key="fail_bar")

        else:
            st.success("No failed checks detected.")

    # --------------------------------------------------------
    # AI Analysis
    # --------------------------------------------------------
    st.subheader("AI Root Cause Analysis")

    if latest:
        ai = latest.get("ai_analysis", {})

        st.write(ai.get("analysis_summary", "No summary available"))

        for i, issue in enumerate(ai.get("issues", []), 1):
            with st.expander(f"Issue {i}: {issue.get('check_name')}"):
                st.write(f"Severity: {issue.get('severity')}")
                st.write(f"Root Cause: {issue.get('root_cause')}")
                st.write(f"Fix: {issue.get('recommended_fix')}")

    # --------------------------------------------------------
    # Historical Runs
    # --------------------------------------------------------
    st.subheader("Historical Runs")
    st.dataframe(df, use_container_width=True)

    # --------------------------------------------------------
    # Download
    # --------------------------------------------------------
    csv = df.to_csv(index=False).encode("utf-8")

    st.download_button(
        "Download CSV",
        csv,
        "dq_runs.csv",
        "text/csv"
    )


if __name__ == "__main__":
    main()