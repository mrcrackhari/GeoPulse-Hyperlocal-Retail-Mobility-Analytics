from __future__ import annotations

import os
import numpy as np
import pandas as pd
import streamlit as st


ZONE_PROFILES: dict[str, dict[str, float]] = {
    "Central": {"multiplier": 1.18, "weekend_boost": 1.24},
    "East": {"multiplier": 1.06, "weekend_boost": 1.15},
    "South": {"multiplier": 0.94, "weekend_boost": 1.08},
    "North-East": {"multiplier": 0.89, "weekend_boost": 1.12},
    "South-Central": {"multiplier": 0.86, "weekend_boost": 0.96},
    "North": {"multiplier": 0.82, "weekend_boost": 1.05},
}


def _has_snowflake_env() -> bool:
    required = ["SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD"]
    return all(bool(os.getenv(k, "").strip()) for k in required)


@st.cache_data(ttl=120, show_spinner=False)
def _fetch_snowflake_staging() -> tuple[pd.DataFrame | None, pd.DataFrame | None, str]:
    if not _has_snowflake_env():
        return None, None, "Snowflake env vars not set; using Day 5 profile fallback"

    try:
        import snowflake.connector

        conn = snowflake.connector.connect(
            account=os.environ["SNOWFLAKE_ACCOUNT"].strip(),
            user=os.environ["SNOWFLAKE_USER"].strip(),
            password=os.environ["SNOWFLAKE_PASSWORD"].strip(),
            role=os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN").strip(),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "GEOPULSE_WH").strip(),
            database=os.getenv("SNOWFLAKE_DATABASE", "GEOPULSE_DB").strip(),
            schema=os.getenv("SNOWFLAKE_SCHEMA", "RAW").strip(),
        )
        try:
            pings_df = pd.read_sql("SELECT * FROM GEOPULSE_DB.RAW.stg_gps_pings LIMIT 50000", conn)
            stores_df = pd.read_sql("SELECT * FROM GEOPULSE_DB.RAW.stg_stores LIMIT 5000", conn)
        finally:
            conn.close()

        if pings_df.empty and stores_df.empty:
            return None, None, "Connected to Snowflake, but staging tables are empty; using fallback"
        return pings_df, stores_df, "Live Snowflake GEOPULSE_DB.RAW staging data active"
    except Exception as exc:
        return None, None, f"Snowflake fallback active ({type(exc).__name__})"


@st.cache_data(show_spinner=False)
def _generate_hourly_series(seed: int, day_type: str) -> pd.DataFrame:
    hours = list(range(24))
    rng = np.random.default_rng(seed)

    if day_type == "Weekend":
        base = np.array([
            140, 95, 70, 60, 65, 95, 175, 310, 540, 820, 1120, 1360,
            1540, 1620, 1580, 1640, 1820, 2050, 2240, 2180, 1890, 1420, 920, 480
        ])
    else:
        base = np.array([
            190, 125, 85, 70, 90, 165, 360, 690, 1080, 1190, 1060, 1090,
            1180, 1140, 1010, 1080, 1340, 1760, 2120, 2040, 1690, 1140, 640, 310
        ])

    jitter = rng.integers(-55, 75, size=len(hours))
    pings = np.clip(base + jitter, 40, None)
    return pd.DataFrame({
        "hour": hours,
        "hour_label": [f"{h:02d}:00" for h in hours],
        "day_type": day_type,
        "gps_pings": pings,
    })


@st.cache_data(show_spinner=False)
def get_store_footfall_data(day_type: str = "Weekday") -> pd.DataFrame:
    weekend_factor = 1.14 if day_type == "Weekend" else 1.0
    df = pd.DataFrame({
        "store_id": ["ST-101", "ST-102", "ST-103", "ST-104", "ST-105", "ST-106"],
        "store_name": [
            "Hazratganj Flagship",
            "Gomti Nagar Hub",
            "Alambagh Market Store",
            "Indira Nagar Express",
            "Charbagh Transit Store",
            "Mahanagar Corner",
        ],
        "zone": ["Central", "East", "South", "North-East", "South-Central", "North"],
        "base_footfall": [4820, 4390, 3710, 3480, 3290, 2940],
        "avg_dwell_mins": [24.5, 21.0, 16.5, 18.0, 12.5, 19.5],
    })
    df["estimated_footfall"] = np.round(df["base_footfall"] * weekend_factor).astype(int)
    df["dwell_segment"] = np.where(
        df["avg_dwell_mins"] >= 22.0,
        "High Engagement (>22m)",
        np.where(df["avg_dwell_mins"] >= 15.0, "Browsing (15-22m)", "Quick Stop (<15m)"),
    )
    df["engagement_index"] = np.round((df["estimated_footfall"] * df["avg_dwell_mins"]) / 1000.0, 1)
    return df[[
        "store_id",
        "store_name",
        "zone",
        "dwell_segment",
        "estimated_footfall",
        "avg_dwell_mins",
        "engagement_index",
    ]]


def get_placeholder_hourly_mobility(
    store_filter: str = "All Stores",
    zone_filter: str = "All Zones",
    day_type: str = "Weekday",
) -> pd.DataFrame:
    key = f"{store_filter}|{zone_filter}|{day_type}"
    seed = 105 if key == "All Stores|All Zones|Weekday" else (abs(hash(key)) % 100000)
    df = _generate_hourly_series(seed, day_type).copy()

    mult = 1.0
    if zone_filter != "All Zones" and zone_filter in ZONE_PROFILES:
        profile = ZONE_PROFILES[zone_filter]
        mult = profile["multiplier"] * (profile["weekend_boost"] if day_type == "Weekend" else 1.0)

    if store_filter != "All Stores":
        stores = get_store_footfall_data(day_type)
        match = stores[stores["store_name"] == store_filter]
        if not match.empty:
            store_scale = float(match["estimated_footfall"].iloc[0]) / float(stores["estimated_footfall"].mean())
            mult *= round(store_scale, 3)

    df["gps_pings"] = np.round(df["gps_pings"] * mult).astype(int)
    return df


def load_mobility_data(
    store_filter: str = "All Stores",
    zone_filter: str = "All Zones",
    min_dwell_mins: float = 0.0,
    day_type: str = "Weekday",
) -> tuple[pd.DataFrame, pd.DataFrame, str]:
    _, _, sf_msg = _fetch_snowflake_staging()

    hourly_df = get_placeholder_hourly_mobility(
        store_filter=store_filter,
        zone_filter=zone_filter,
        day_type=day_type,
    )
    stores_df = get_store_footfall_data(day_type=day_type).copy()

    if zone_filter != "All Zones":
        stores_df = stores_df[stores_df["zone"] == zone_filter]

    if min_dwell_mins > 0.0:
        stores_df = stores_df[stores_df["avg_dwell_mins"] >= min_dwell_mins]

    if store_filter != "All Stores":
        matched = stores_df[stores_df["store_name"] == store_filter]
        if not matched.empty:
            stores_df = matched

    stores_df = stores_df.reset_index(drop=True)
    status = (
        f"{sf_msg} | {day_type} | Zone: {zone_filter} | "
        f"Store: {store_filter} | Min Dwell: {min_dwell_mins:.1f}m"
    )
    return hourly_df, stores_df, status


def get_mobility_summary(hourly_df: pd.DataFrame, stores_df: pd.DataFrame) -> dict[str, str]:
    total_pings = int(hourly_df["gps_pings"].sum()) if not hourly_df.empty else 0
    if not hourly_df.empty:
        peak_row = hourly_df.loc[hourly_df["gps_pings"].idxmax()]
        peak_hour = str(peak_row["hour_label"])
        peak_pings = int(peak_row["gps_pings"])
    else:
        peak_hour = "N/A"
        peak_pings = 0

    total_footfall = int(stores_df["estimated_footfall"].sum()) if not stores_df.empty else 0
    avg_dwell = float(stores_df["avg_dwell_mins"].mean()) if not stores_df.empty else 0.0
    avg_engagement = float(stores_df["engagement_index"].mean()) if not stores_df.empty else 0.0

    return {
        "window_pings": f"{total_pings:,}",
        "peak_hour": peak_hour,
        "peak_pings": f"{peak_pings:,} pings at peak",
        "filtered_footfall": f"{total_footfall:,}",
        "store_count": f"{len(stores_df)} stores in view",
        "avg_dwell": f"{avg_dwell:.1f} min",
        "avg_engagement": f"{avg_engagement:.1f} avg engagement",
    }