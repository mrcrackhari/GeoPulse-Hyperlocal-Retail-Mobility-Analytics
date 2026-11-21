from __future__ import annotations

from pathlib import Path
import sys
import pandas as pd
import streamlit as st

DASHBOARD_DIR = Path(__file__).resolve().parent
if str(DASHBOARD_DIR) not in sys.path:
    sys.path.insert(0, str(DASHBOARD_DIR))

from data_loader import (
    get_placeholder_kpis,
    get_store_footfall_data,
    load_mobility_data,
)


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
    st.caption("Day 5 Visualization Lead - Interactive Filters, Dwell Analytics & Cached Data Layer")

    catalog_df = get_store_footfall_data()
    zone_options = ["All Zones"] + sorted(catalog_df["zone"].unique().tolist())

    with st.sidebar:
        st.header("Filters")
        selected_zone = st.selectbox("Retail Zone", options=zone_options, index=0)

        filtered_catalog = (
            catalog_df if selected_zone == "All Zones"
            else catalog_df[catalog_df["zone"] == selected_zone]
        )
        store_options = ["All Stores"] + filtered_catalog["store_name"].tolist()
        selected_store = st.selectbox("Store Location", options=store_options, index=0)

        window_mode = st.radio(
            "Time Window",
            options=["Full Day (24h)", "Peak Hours (17:00-21:00)"],
            index=0,
        )
        min_dwell = st.slider(
            "Minimum Avg Dwell (mins)",
            min_value=0.0,
            max_value=25.0,
            value=0.0,
            step=2.5,
        )

    hourly_df, stores_df, source_status = load_mobility_data(
        store_filter=selected_store,
        zone_filter=selected_zone,
        min_dwell_mins=min_dwell,
    )
    if window_mode == "Peak Hours (17:00-21:00)":
        hourly_df = hourly_df[hourly_df["hour"].between(17, 21)].reset_index(drop=True)

    st.markdown(f'<div class="status-banner">{source_status}</div>', unsafe_allow_html=True)

    kpis = get_placeholder_kpis()
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total GPS Pings", str(kpis["total_gps_pings"]), str(kpis["total_gps_pings_delta"]))
    col2.metric("Active Stores", str(kpis["active_stores"]), str(kpis["active_stores_delta"]))
    col3.metric("Peak Mobility Window", str(kpis["peak_mobility_window"]), str(kpis["peak_mobility_delta"]))
    col4.metric("Staging Models", str(kpis["staging_models_ready"]), str(kpis["staging_models_delta"]))

    tab_overview, tab_stores, tab_pipeline = st.tabs([
        "Overview",
        "Store Footfall & Dwell",
        "Staging & Pipeline",
    ])

    with tab_overview:
        st.subheader("Hourly GPS Ping Volume")
        if hourly_df.empty:
            st.info("No hourly mobility rows match the active filter selection.")
        else:
            st.line_chart(hourly_df.set_index("hour_label")[["gps_pings"]], height=320)

    with tab_stores:
        st.subheader("Estimated Store Footfall")
        if stores_df.empty:
            st.info("No stores match the current zone or minimum dwell filter.")
        else:
            st.bar_chart(stores_df.set_index("store_name")[["estimated_footfall"]], height=300)
            st.subheader("Average Dwell Time by Store (mins)")
            st.bar_chart(stores_df.set_index("store_name")[["avg_dwell_mins"]], height=240)
            st.dataframe(stores_df, width="stretch", hide_index=True)

    with tab_pipeline:
        st.subheader("Snowflake & dbt Staging Readiness")
        pipeline_df = pd.DataFrame({
            "Component": [
                "GEOPULSE_DB.RAW.stg_gps_pings",
                "GEOPULSE_DB.RAW.stg_stores",
                "Dashboard Data Loader",
            ],
            "Status": [
                "View Built (Day 3)",
                "View Built (Day 3)",
                "Cached Placeholder Active (Day 5)",
            ],
            "Next Step": [
                "Connect live row metrics",
                "Connect store attributes",
                "Swap in mart/staging queries",
            ],
        })
        st.dataframe(pipeline_df, width="stretch", hide_index=True)


if __name__ == "__main__":
    main()