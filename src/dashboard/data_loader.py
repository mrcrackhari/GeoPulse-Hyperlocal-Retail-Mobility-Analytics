from __future__ import annotations

import numpy as np
import pandas as pd
import streamlit as st


def get_placeholder_kpis() -> dict[str, object]:
    return {
        "total_gps_pings": "1,248,500",
        "total_gps_pings_delta": "+14.2% vs prior window (placeholder)",
        "active_stores": 48,
        "active_stores_delta": "100% mapped in staging hook",
        "peak_mobility_window": "18:00 - 20:00",
        "peak_mobility_delta": "Evening retail rush (placeholder)",
        "staging_models_ready": "2 / 2",
        "staging_models_delta": "stg_gps_pings & stg_stores",
    }


@st.cache_data(show_spinner=False)
def _build_hourly_curve(seed: int) -> pd.DataFrame:
    hours = list(range(24))
    rng = np.random.default_rng(seed)
    base_curve = np.array([
        180, 120, 90, 75, 85, 140, 290, 520, 780, 910, 960, 1040,
        1120, 1090, 980, 1020, 1210, 1540, 1890, 1960, 1680, 1180, 690, 340
    ])
    noise = rng.integers(-60, 80, size=len(hours))
    pings = np.clip(base_curve + noise, 50, None)
    return pd.DataFrame({
        "hour": hours,
        "hour_label": [f"{h:02d}:00" for h in hours],
        "gps_pings": pings,
    })


@st.cache_data(show_spinner=False)
def get_store_footfall_data() -> pd.DataFrame:
    return pd.DataFrame({
        "store_name": [
            "Hazratganj Flagship",
            "Gomti Nagar Hub",
            "Alambagh Market Store",
            "Indira Nagar Express",
            "Charbagh Transit Store",
            "Mahanagar Corner",
        ],
        "zone": ["Central", "East", "South", "North-East", "South-Central", "North"],
        "estimated_footfall": [4820, 4390, 3710, 3480, 3290, 2940],
        "avg_dwell_mins": [24.5, 21.0, 16.5, 18.0, 12.5, 19.5],
    })


def get_placeholder_hourly_mobility(
    store_filter: str = "All Stores",
    zone_filter: str = "All Zones",
) -> pd.DataFrame:
    key_str = f"{store_filter}|{zone_filter}"
    seed = 42 if key_str == "All Stores|All Zones" else (abs(hash(key_str)) % 10000)
    df = _build_hourly_curve(seed).copy()
    if zone_filter != "All Zones":
        zone_multipliers = {
            "Central": 1.15,
            "East": 1.05,
            "South": 0.92,
            "North-East": 0.88,
            "South-Central": 0.85,
            "North": 0.80,
        }
        mult = zone_multipliers.get(zone_filter, 1.0)
        df["gps_pings"] = np.round(df["gps_pings"] * mult).astype(int)
    return df


def load_mobility_data(
    store_filter: str = "All Stores",
    zone_filter: str = "All Zones",
    min_dwell_mins: float = 0.0,
) -> tuple[pd.DataFrame, pd.DataFrame, str]:
    hourly_df = get_placeholder_hourly_mobility(
        store_filter=store_filter,
        zone_filter=zone_filter,
    )
    stores_df = get_store_footfall_data().copy()

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
        f"Day 5 cached placeholder layer active | Zone: {zone_filter} | "
        f"Store: {store_filter} | Min Dwell: {min_dwell_mins:.1f}m"
    )
    return hourly_df, stores_df, status