from itertools import product
import os
from typing import Tuple
import pandas as pd

import os
import pandas as pd

from app.config import PROJECT_ROOT


# -------------------------
# 1. Load data
# -------------------------
def load_data(folder_name: str, init_file: str, changed_file: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Load and preprocess initial and change datasets from CSV files.

    Parameters
    ----------
    folder_name : str
        Directory containing input files.
    init_file : str
        Filename of the initial dataset (creation data).
    changed_file : str
        Filename of the change events dataset.

    Returns
    -------
    created_csv : pd.DataFrame
        Initial dataset with parsed datetime in `creation_date`.
    changed_csv : pd.DataFrame
        Change events dataset with parsed datetime in `date`.
    """
    created_csv = pd.read_csv(os.path.join(folder_name, init_file), index_col=False)
    changed_csv = pd.read_csv(os.path.join(folder_name, changed_file), index_col=False)

    created_csv["creation_date"] = pd.to_datetime(created_csv["creation_date"])
    changed_csv["date"] = pd.to_datetime(changed_csv["date"])

    return created_csv, changed_csv


# -------------------------
# 2. Build unified state table
# -------------------------
def build_states(created_csv: pd.DataFrame, changed_csv: pd.DataFrame) -> pd.DataFrame:
    """
    Build a unified state history table from initial and change events.

    This function merges initial license states with update events,
    ensuring that only the latest state per (license_id, date) is kept.

    Parameters
    ----------
    created_csv : pd.DataFrame
        Initial license creation dataset.
    changed_csv : pd.DataFrame
        License state change events dataset.

    Returns
    -------
    states : pd.DataFrame
        Combined state history with columns:
        [license_id, date, renewable, price, type, id]
    """
    changed_clean = (
        changed_csv
        .sort_values(["license_id", "date", "id"])
        .groupby(["license_id", "date"])
        .tail(1)
    )

    initial_state = (
        created_csv
        .rename(columns={"id": "license_id", "creation_date": "date"})
        [["license_id", "date", "renewable", "price", "type"]]
    )

    initial_state["id"] = -1
    changed_index = changed_clean.set_index(["license_id", "date"]).index
    initial_state = initial_state[
        ~initial_state.set_index(["license_id", "date"]).index.isin(changed_index)
    ]

    states = (
        pd.concat([changed_clean, initial_state])
        .sort_values(["license_id", "date", "id"])
    )

    return states

def build_full_daily_states(states, created_csv):
    """
    Build a complete panel dataset covering all dates and all licenses.

    Ensures each (date, license_id) pair exists and applies creation
    constraints to avoid invalid pre-creation dates.

    Parameters
    ----------
    states : pd.DataFrame
        State transition table.
    created_csv : pd.DataFrame
        Initial dataset containing license creation dates.

    Returns
    -------
    full : pd.DataFrame
        Fully expanded daily state panel with forward-filled values.
    all_dates : pd.DatetimeIndex
        Complete date range of the dataset.
    """
    states = states.copy()
    states["date"] = pd.to_datetime(states["date"])

    # -------------------------
    # 1. Full date range
    # -------------------------
    all_dates = pd.date_range(states["date"].min(), states["date"].max())

    # -------------------------
    # 2. Full license universe
    # -------------------------
    all_licenses = created_csv["id"].unique()

    # -------------------------
    # 3. Build full grid (date x license)
    # -------------------------
    full_grid = pd.DataFrame(
    list(product(all_dates, created_csv["id"].unique())),
    columns=["date", "license_id"]
)

    # attach creation_date
    full_grid = full_grid.merge(
        created_csv[["id", "creation_date"]],
        left_on="license_id",
        right_on="id",
        how="left"
    ).drop(columns="id")
    full_grid = full_grid[full_grid["date"] >= full_grid["creation_date"]]

    # -------------------------
    # 4. Clean state events (not sure it is very useful)
    # -------------------------
    states = states.sort_values(["license_id", "date", "id"])
    states = states.drop_duplicates(["license_id", "date"], keep="last")

    # -------------------------
    # 5. Merge + forward fill per license
    # -------------------------
    full = full_grid.merge(states, on=["date", "license_id"], how="left")
    full = full.sort_values(["license_id", "date"])

    full[["renewable", "type", "price"]] = (
        full.groupby("license_id")[["renewable", "type", "price"]]
        .ffill()
    )


    return full, all_dates
# -------------------------
# 3. State transitions
# -------------------------
def compute_counts(daily_states: pd.DataFrame,  all_dates: pd.DatetimeIndex) -> pd.DataFrame:
    """
    Compute active and inactive license counts per day and type.

    Also computes daily deltas for active and inactive counts.

    Parameters
    ----------
    daily_states : pd.DataFrame
        Daily state-expanded dataset.
    all_dates : pd.DatetimeIndex
        Full range of dates in dataset.

    Returns
    -------
    df : pd.DataFrame
        Aggregated daily counts and differences with columns:
        - active_license_count
        - inactive_license_count
        - daily_active_diff
        - daily_inactive_diff
    """
    # -------------------------
    # 1. Active / inactive from state (ground truth)
    # -------------------------
    daily_states = (
        daily_states
        .sort_values(["date"])  # IMPORTANT: add timestamp if available (see below)
        .drop_duplicates(subset=["date", "license_id"], keep="last")
    )
    active = (
        daily_states[daily_states["renewable"] == True]
        .groupby(["date", "type"])
        .size()
    )

    inactive = (
        daily_states[daily_states["renewable"] == False]
        .groupby(["date", "type"])
        .size()
    )
    # -------------------------
    # 2. Base grid (all date/type combinations)
    # -------------------------
    all_types = daily_states["type"].unique()

    df = pd.DataFrame(
    list(product(all_dates, all_types)),
    columns=["date", "type"]
)

    # -------------------------
    # 3. Map counts (NO subtraction anywhere)
    # -------------------------
    df["active_license_count"] = df.set_index(["date", "type"]).index.map(active).fillna(0).values
    df["inactive_license_count"] = df.set_index(["date", "type"]).index.map(inactive).fillna(0).values

    df["active_license_count"] = df["active_license_count"].astype(int)
    df["inactive_license_count"] = df["inactive_license_count"].astype(int)

    # -------------------------
    # 4. Daily diffs (safe now)
    # -------------------------
    df = df.sort_values(["type", "date"])

    df["daily_active_diff"] = df.groupby("type")["active_license_count"].diff().fillna(0)
    df["daily_inactive_diff"] = df.groupby("type")["inactive_license_count"].diff().fillna(0)

    return df


# -------------------------
# 4. Daily event aggregation
# -------------------------
def compute_daily_events(states: pd.DataFrame, all_dates: pd.DatetimeIndex) -> pd.DataFrame:
    """
    Aggregate activation and deactivation events per day and type.

    Parameters
    ----------
    states : pd.DataFrame
        State history containing activation flags.
    all_dates : pd.DatetimeIndex
        Full date range.

    Returns
    -------
    daily_events : pd.DataFrame
        Event counts per (date, type).
    """
    daily_events = (
        states.groupby(["date", "type"])
        .agg(
            activated_licenses=("activated", "sum"),
            deactivated_licenses=("deactivated", "sum")
        )
        .reindex(pd.MultiIndex.from_product(
            [all_dates, states["type"].unique()],
            names=["date", "type"]
        ), fill_value=0)
        .reset_index()
    )

    return daily_events


# -------------------------
# 5. Active / inactive calculations
# -------------------------


def expand_license_daily(df: pd.DataFrame) -> pd.Series:
    """
    Expand a single license's renewal status into a daily time series.

    Parameters
    ----------
    df : pd.DataFrame
        License-level state data.

    Returns
    -------
    pd.Series
        Daily forward-filled renewal status indexed by date.
    """
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date")

    return (
        df.set_index("date")["renewable"]
        .resample("D")
        .ffill()
    )
# -------------------------
# 6. Price computation
# -------------------------
def compute_price(daily_states: pd.DataFrame, all_dates: pd.DatetimeIndex) -> pd.DataFrame:
    """
    Compute total active license price per day and type.

    Active value is defined as:
        renewable * price

    Parameters
    ----------
    daily_states : pd.DataFrame
        Daily expanded state dataset.
    all_dates : pd.DatetimeIndex
        Full date range.

    Returns
    -------
    daily_price : pd.DataFrame
        Aggregated active license price per (date, type).
    """
    df = daily_states.copy()

    df["active_value"] = df["renewable"] * df["price"]

    daily_price = (
        df.groupby(["date", "type"])["active_value"]
        .sum()
        .reindex(
            pd.MultiIndex.from_product([all_dates, df["type"].unique()],
            names=["date", "type"]),
            fill_value=0
        )
        .reset_index(name="active_license_price")
    )

    return daily_price

def compute_daily_price_diff(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute daily differences in active license price per type.

    Parameters
    ----------
    df : pd.DataFrame
        Dataset containing `active_license_price`.

    Returns
    -------
    df : pd.DataFrame
        Dataset with additional column:
        - daily_price_diff
    """
    # df = df.copy()

    # ensure correct order for diff computation
    df = df.sort_values(["type", "date"])

    df["daily_price_diff"] = (
        df.groupby("type")["active_license_price"]
        .diff()
        .fillna(0)
    )

    # final display order
    df = df.sort_values(["date", "type"])

    return df
# -------------------------
# 7. Full pipeline
# -------------------------
def transform_pipeline(init: pd.DataFrame, changed: pd.DataFrame) -> pd.DataFrame:
    """
    End-to-end ETL pipeline for license state analytics.

    Steps:
    1. Build unified state history
    2. Expand to full daily panel
    3. Compute active/inactive counts
    4. Compute price aggregation
    5. Merge metrics
    6. Compute daily deltas

    Parameters
    ----------
    init : pd.DataFrame
        Initial license dataset.
    changed : pd.DataFrame
        License change event dataset.

    Returns
    -------
    pd.DataFrame
        Final daily analytics table with columns:
        - date
        - type
        - active_license_count
        - active_license_price
        - inactive_license_count
        - daily_active_diff
        - daily_price_diff
        - daily_inactive_diff
    """

    states = build_states(init, changed)

    daily_states, all_dates = build_full_daily_states(states, init)

    daily_events = compute_counts(daily_states,  all_dates)

    daily_price = compute_price(daily_states, all_dates)

    daily_events = daily_events.merge(daily_price, on=["date", "type"], how="left")

    
    daily_events = compute_daily_price_diff(daily_events)
    transformed_col = ("date", "type", "active_license_count", "active_license_price", "inactive_license_count",
                   "daily_active_diff", "daily_price_diff", "daily_inactive_diff",)
    
    return daily_events[list(transformed_col)]


if __name__ == "__main__":
    dir_path = PROJECT_ROOT
    folder_name=os.path.join(dir_path, "csv")
    init_file="initial_licenses.csv"
    changed_file="license_changes.csv"
    output_file="transformed.csv"
    created_csv, changed_csv = load_data(folder_name, init_file, changed_file)
    result = transform_pipeline(created_csv, changed_csv )
    result.to_csv(os.path.join(folder_name, output_file), index=False)
    print("data now transformed and saved at", os.path.join(folder_name, output_file))
