from __future__ import annotations

import numpy as np
import pandas as pd


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


def get_placeholder_hourly_mobility(store_filter: str = "All Stores") -> pd.DataFrame:
    hours = list(range(24))
    seed = 42 if store_filter == "All Stores" else abs(hash(store_filter)) % 10000
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


def get_placeholder_store_footfall() -> pd.DataFrame:
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


def load_mobility_data(store_filter: str = "All Stores") -> tuple[pd.DataFrame, pd.DataFrame, str]:
    hourly_df = get_placeholder_hourly_mobility(store_filter=store_filter)
    stores_df = get_placeholder_store_footfall()
    if store_filter != "All Stores":
        filtered_stores = stores_df[stores_df["store_name"] == store_filter]
        if not filtered_stores.empty:
            stores_df = filtered_stores.reset_index(drop=True)
    return hourly_df, stores_df, "Placeholder dataset active (Snowflake GEOPULSE_DB staging hook ready)"