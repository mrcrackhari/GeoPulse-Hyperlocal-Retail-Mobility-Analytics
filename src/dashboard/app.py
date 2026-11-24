from __future__ import annotations

from pathlib import Path
import sys
import pandas as pd
import streamlit as st

DASHBOARD_DIR = Path(__file__).resolve().parent
if str(DASHBOARD_DIR) not in sys.path:
    sys.path.insert(0, str(DASHBOARD_DIR))

from data_loader import (
    get_mobility_summary,
    get_placeholder_hourly_mobility,
    get_store_footfall_data,
    get_zone_summary,
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
            margin-bottom: 0.75rem;
        }
        .insight-banner {
            background: rgba(16, 42, 67, 0.72);
            backdrop-filter: blur(10px);
            border: 1px solid rgba(56, 189, 248, 0.28);
            border-radius: 12px;
            padding: 10px 14px;
            font-size: 0.92rem;
            color: #e0f2fe;
            margin-bottom: 1rem;
        }
    </style>
    """,
    unsafe_allow_html=True,
)


def main() -> None:
    st.title("GeoPulse Hyperlocal Retail Mobility Analytics")
    st.caption("Day 7 Visualization Lead - Zone Rollups, Store Rankings & Mobility Takeaways")

    with st.sidebar:
        st.header("Filters")
        if st.button("Reset Filters", width="stretch"):
            st.session_state["day_type"] = "Weekday"
            st.session_state["selected_zone"] = "All Zones"
            st.session_state["selected_store"] = "All Stores"
            st.session_state["window_mode"] = "Full Day (24h)"
            st.session_state["min_dwell"] = 0.0

        day_type = st.radio("Day Profile", options=["Weekday", "Weekend"], index=0, key="day_type")

        catalog_df = get_store_footfall_data(day_type=day_type)
        zone_options = ["All Zones"] + sorted(catalog_df["zone"].unique().tolist())
        selected_zone = st.selectbox("Retail Zone", options=zone_options, index=0, key="selected_zone")

        filtered_catalog = (
            catalog_df if selected_zone == "All Zones"
            else catalog_df[catalog_df["zone"] == selected_zone]
        )
        store_options = ["All Stores"] + filtered_catalog["store_name"].tolist()
        if st.session_state.get("selected_store", "All Stores") not in store_options:
            st.session_state["selected_store"] = "All Stores"
        selected_store = st.selectbox("Store Location", options=store_options, index=0, key="selected_store")

        window_mode = st.radio(
            "Time Window",
            options=["Full Day (24h)", "Peak Hours (17:00-21:00)"],
            index=0,
            key="window_mode",
        )
        min_dwell = st.slider(
            "Minimum Avg Dwell (mins)",
            min_value=0.0,
            max_value=25.0,
            value=0.0,
            step=2.5,
            key="min_dwell",
        )

    hourly_df, stores_df, source_status = load_mobility_data(
        store_filter=selected_store,
        zone_filter=selected_zone,
        min_dwell_mins=min_dwell,
        day_type=day_type,
    )
    if window_mode == "Peak Hours (17:00-21:00)":
        hourly_df = hourly_df[hourly_df["hour"].between(17, 21)].reset_index(drop=True)

    summary = get_mobility_summary(hourly_df, stores_df)
    zone_df = get_zone_summary(stores_df)

    st.markdown(f'<div class="status-banner">{source_status}</div>', unsafe_allow_html=True)
    st.markdown(
        f'<div class="insight-banner"><strong>Key Takeaway:</strong> Peak activity hits at '
        f'<strong>{summary["peak_hour"]}</strong> ({summary["peak_pings"]}) on <strong>{day_type}s</strong>; '
        f'top engagement leader in view: <strong>{summary["top_store"]}</strong>.</div>',
        unsafe_allow_html=True,
    )

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Window GPS Pings", summary["window_pings"], f"{day_type} | {window_mode}")
    col2.metric("Filtered Footfall", summary["filtered_footfall"], summary["store_count"])
    col3.metric("Peak Hour", summary["peak_hour"], summary["peak_pings"])
    col4.metric("Avg Store Dwell", summary["avg_dwell"], summary["avg_engagement"])

    tab_overview, tab_stores, tab_zones, tab_pipeline = st.tabs([
        "Overview & Day Comparison",
        "Store Rankings & Dwell",
        "Zone Rollup",
        "Staging & Pipeline",
    ])

    with tab_overview:
        st.subheader(f"Hourly GPS Ping Volume ({day_type})")
        if hourly_df.empty:
            st.info("No hourly mobility rows match the active filter selection.")
        else:
            st.line_chart(hourly_df.set_index("hour_label")[["gps_pings"]], height=300)

            st.subheader("Weekday vs. Weekend Hourly Comparison")
            weekday_df = get_placeholder_hourly_mobility(
                store_filter=selected_store,
                zone_filter=selected_zone,
                day_type="Weekday",
            )
            weekend_df = get_placeholder_hourly_mobility(
                store_filter=selected_store,
                zone_filter=selected_zone,
                day_type="Weekend",
            )
            if window_mode == "Peak Hours (17:00-21:00)":
                weekday_df = weekday_df[weekday_df["hour"].between(17, 21)]
                weekend_df = weekend_df[weekend_df["hour"].between(17, 21)]

            compare_df = pd.DataFrame({
                "Weekday": weekday_df.set_index("hour_label")["gps_pings"],
                "Weekend": weekend_df.set_index("hour_label")["gps_pings"],
            })
            st.line_chart(compare_df, height=280)

    with tab_stores:
        st.subheader("Ranked Store Footfall & Engagement Index")
        if stores_df.empty:
            st.info("No stores match the current zone or minimum dwell filter.")
        else:
            st.bar_chart(stores_df.set_index("store_name")[["estimated_footfall", "engagement_index"]], height=300)

            st.subheader("Dwell Segment Distribution")
            segment_counts = (
                stores_df.groupby("dwell_segment", as_index=False)["store_id"]
                .count()
                .rename(columns={"store_id": "stores"})
            )
            st.bar_chart(segment_counts.set_index("dwell_segment")[["stores"]], height=220)

            st.dataframe(stores_df, width="stretch", hide_index=True)
            csv_bytes = stores_df.to_csv(index=False).encode("utf-8")
            st.download_button(
                label="Download Filtered Store Metrics (CSV)",
                data=csv_bytes,
                file_name=f"geopulse_store_metrics_{day_type.lower()}.csv",
                mime="text/csv",
            )

    with tab_zones:
        st.subheader("Zone-Level Footfall & Engagement Summary")
        if zone_df.empty:
            st.info("No zone metrics available for the active filter selection.")
        else:
            st.bar_chart(zone_df.set_index("zone")[["total_footfall", "avg_engagement_index"]], height=290)
            st.dataframe(zone_df, width="stretch", hide_index=True)

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
                "Day 7 Zone Rollup & Ranking Active",
            ],
            "Next Step": [
                "Query live pings when loaded",
                "Query live stores when loaded",
                "Swap in final MARTS models",
            ],
        })
        st.dataframe(pipeline_df, width="stretch", hide_index=True)


if __name__ == "__main__":
    main()