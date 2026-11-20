from __future__ import annotations

from pathlib import Path
import sys
import pandas as pd
import streamlit as st

DASHBOARD_DIR = Path(__file__).resolve().parent
if str(DASHBOARD_DIR) not in sys.path:
    sys.path.insert(0, str(DASHBOARD_DIR))

from data_loader import get_placeholder_kpis, load_mobility_data


st.set_page_config(
    page_title="GeoPulse | Hyperlocal Retail Mobility",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown(
    """
    <style>
        .stApp {
            background: radial-gradient(circle at top left, #132238 0%, #0b111e 55%, #070b14 100%);
            color: #e6edf7;
        }
        [data-testid="stSidebar"] {
            background: rgba(15, 23, 42, 0.78);
            backdrop-filter: blur(12px);
            border-right: 1px solid rgba(148, 163, 184, 0.16);
        }
        [data-testid="stMetric"] {
            background: rgba(20, 30, 48, 0.68);
            backdrop-filter: blur(12px);
            border: 1px solid rgba(148, 163, 184, 0.18);
            border-radius: 14px;
            padding: 14px 18px;
        }
        .status-banner {
            background: rgba(30, 41, 59, 0.65);
            backdrop-filter: blur(10px);
            border: 1px solid rgba(148, 163, 184, 0.2);
            border-radius: 12px;
            padding: 10px 14px;
            font-size: 0.9rem;
            color: #cbd5e1;
            margin-bottom: 1rem;
        }
    </style>
    """,
    unsafe_allow_html=True,
)


def main() -> None:
    st.title("GeoPulse Hyperlocal Retail Mobility Analytics")
    st.caption("Day 4 Visualization Lead Skeleton - Placeholder Metrics, Charts & Staging Hook")

    _, all_stores_df, _ = load_mobility_data("All Stores")
    store_options = ["All Stores"] + all_stores_df["store_name"].tolist()

    with st.sidebar:
        st.header("Filters")
        selected_store = st.selectbox("Store Location", options=store_options, index=0)
        window_mode = st.radio("Time Window", options=["Full Day (24h)", "Peak Hours (17:00-21:00)"], index=0)

    hourly_df, stores_df, source_status = load_mobility_data(store_filter=selected_store)
    if window_mode == "Peak Hours (17:00-21:00)":
        hourly_df = hourly_df[hourly_df["hour"].between(17, 21)].reset_index(drop=True)

    st.markdown(f'<div class="status-banner">{source_status}</div>', unsafe_allow_html=True)

    kpis = get_placeholder_kpis()
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total GPS Pings", str(kpis["total_gps_pings"]), str(kpis["total_gps_pings_delta"]))
    col2.metric("Active Stores", str(kpis["active_stores"]), str(kpis["active_stores_delta"]))
    col3.metric("Peak Mobility Window", str(kpis["peak_mobility_window"]), str(kpis["peak_mobility_delta"]))
    col4.metric("Staging Models", str(kpis["staging_models_ready"]), str(kpis["staging_models_delta"]))

    tab_overview, tab_stores, tab_pipeline = st.tabs(["Overview", "Store Footfall", "Staging & Pipeline"])

    with tab_overview:
        st.subheader("Hourly GPS Ping Volume (Placeholder)")
        chart_df = hourly_df.set_index("hour_label")[["gps_pings"]]
        st.line_chart(chart_df, height=320)

    with tab_stores:
        st.subheader("Estimated Store Footfall (Placeholder)")
        st.bar_chart(stores_df.set_index("store_name")[["estimated_footfall"]], height=300)
        st.dataframe(stores_df, width="stretch", hide_index=True)

    with tab_pipeline:
        st.subheader("Snowflake & dbt Staging Readiness")
        pipeline_df = pd.DataFrame({
            "Component": ["GEOPULSE_DB.RAW.stg_gps_pings", "GEOPULSE_DB.RAW.stg_stores", "Dashboard Data Loader"],
            "Status": ["View Built (Day 3)", "View Built (Day 3)", "Placeholder Active (Day 4)"],
            "Next Step": ["Connect live row metrics", "Connect store attributes", "Swap in mart/staging queries"],
        })
        st.dataframe(pipeline_df, width="stretch", hide_index=True)


if __name__ == "__main__":
    main()