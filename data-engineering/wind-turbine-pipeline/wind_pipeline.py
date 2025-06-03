"""
wind_pipeline.py
================
Data-processing pipeline for daily wind-turbine CSV feeds
(technical task – Senior Data Engineer interview).

Author: Stefano D'Ippolito

2025
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd

################################################################################
# 1. I/O – read & concatenate daily CSVs
################################################################################


def load_and_concat_data(files: Iterable[str | os.PathLike]) -> pd.DataFrame:
    """
    Read multiple CSV files and concatenate them into a single DataFrame.

    Args:
        files: An iterable of file paths (str or Path) pointing to the daily
               CSVs.  Each file must contain at least the columns
               ``timestamp``, ``turbine_id`` and ``power_output`` (MW).

    Returns:
        A single ``pd.DataFrame`` containing the union of all rows plus a
        ``source_file`` column that tracks the origin of each record.

    Raises:
        FileNotFoundError: If any *file* is missing.
        ValueError:        If required columns are absent in a file.

    Notes
    -----
    • Timestamps are **not** parsed here to avoid per-file dtype inference
      overhead.  They are converted once during cleaning.
    """
    frames: list[pd.DataFrame] = []

    for file in files:
        file = Path(file)
        if not file.is_file():
            raise FileNotFoundError(file)

        df = pd.read_csv(file)
        required = {"timestamp", "turbine_id", "power_output"}
        if not required.issubset(df.columns):
            raise ValueError(f"{file.name} is missing columns {required - set(df.columns)}")

        df["source_file"] = file.name
        frames.append(df)

    return pd.concat(frames, ignore_index=True)


################################################################################
# 2. Data cleaning
################################################################################


def clean_data(raw: pd.DataFrame) -> pd.DataFrame:
    """
    Clean the raw turbine data.

    Operations
    ----------
    1. Drop duplicate rows.
    2. Parse timestamps to UTC-aware ``datetime64[ns]``.
    3. Remove rows lacking *turbine_id* **or** *timestamp*.
    4. Impute missing *power_output* with the **per-turbine median**.
    5. Remove obvious outliers using a percentile-based fence.

    Args:
        raw: Un-cleaned DataFrame from :func:`load_and_concat_data`.

    Returns:
        A cleaned DataFrame ready for statistics/anomaly detection.
    """
    df = raw.copy()

    # basic de-dupe
    df = df.drop_duplicates()

    # robust timestamp conversion (errors -> NaT)
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")

    # drop rows that cannot be salvaged
    df = df.dropna(subset=["turbine_id", "timestamp"])

    # ---------- missing value imputation -------------------------------------
    medians = df.groupby("turbine_id", observed=True)["power_output"].transform("median")
    df["power_output"] = df["power_output"].fillna(medians)

    # If still NaNs (if an entire turbine has no valid power readings at all
    # Its median is also NaN, so the fillna() step does nothing): drop – safer than forward/back-fill.
    df = df.dropna(subset=["power_output"])

    # ---------- outlier removal ----------------------------------------------
    # For each turbine build a 1 %–99 % interval and extend by 50 % on each end.
    # Vectorised implementation (no per-row apply):
    q_low = df.groupby("turbine_id", observed=True)["power_output"].quantile(0.01)
    q_high = df.groupby("turbine_id", observed=True)["power_output"].quantile(0.99)

    fences = (
        pd.DataFrame({"low": q_low * 0.5, "high": q_high * 1.5})
        .reset_index()
        .rename_axis(None, axis=1)
    )
    df = df.merge(fences, on="turbine_id", how="left")
    mask = (df["power_output"] >= df["low"]) & (df["power_output"] <= df["high"])
    df = df.loc[mask, raw.columns]  # restore original column order

    # guarantee sorted output for downstream groupby efficiency
    return df.sort_values(["turbine_id", "timestamp"]).reset_index(drop=True)


################################################################################
# 3. Daily summary statistics
################################################################################


def calculate_daily_stats(clean: pd.DataFrame) -> pd.DataFrame:
    """
    Compute min / max / mean power output **per turbine and calendar day**.

    Args:
        clean: DataFrame returned by :func:`clean_data`.

    Returns:
        ``pd.DataFrame`` with columns
        ``turbine_id, date, min_output, max_output, avg_output``.
    """
    data = clean.copy()
    data["date"] = data["timestamp"].dt.date

    stats = (
        data.groupby(["turbine_id", "date"], observed=True)["power_output"]
        .agg(min_output="min", max_output="max", avg_output="mean")
        .reset_index()
    )
    return stats


################################################################################
# 4. Point-level anomaly detection
################################################################################


def detect_anomalies(clean: pd.DataFrame) -> pd.DataFrame:
    """
    Flag records where a turbine’s power output is ±2 σ away from
    its **same-day** mean.

    Args:
        clean: DataFrame returned by :func:`clean_data`.

    Returns:
        A DataFrame with the original columns plus ``is_anomaly`` (bool).

    Implementation details
    ----------------------
    • Daily means/σ are broadcast via a merge (no Python loops).
    • If day-level σ == 0 (constant output), no anomalies are flagged to
      avoid false positives due to division-by-zero.
    """
    work = clean.copy()
    work["date"] = work["timestamp"].dt.date

    stats = (
        work.groupby(["turbine_id", "date"], observed=True)["power_output"]
        .agg(mean="mean", std="std")
        .reset_index()
    )
    work = work.merge(stats, on=["turbine_id", "date"], how="left")

    # avoid NaN or zero std – treat as non-anomalous
    tol = 2 * work["std"].fillna(0)

    # If tol = 0, then any deviation at all would count as an anomaly,
    # even perfectly valid ones (no measurable variation, we can’t call
    # anything “abnormal.”)
    tol.replace(0, np.inf, inplace=True)

    work["is_anomaly"] = (np.abs(work["power_output"] - work["mean"]) > tol)

    return work.drop(columns=["mean", "std"])


################################################################################
# 5. Persistence helpers
################################################################################


def save_outputs(
    cleaned: pd.DataFrame,
    daily_stats: pd.DataFrame,
    anomalies: pd.DataFrame,
    out_dir: str | os.PathLike = "output",
) -> None:
    """
    Persist pipeline artefacts to disk (CSV files).

    Args:
        cleaned:   Output from :func:`clean_data`.
        daily_stats: Output from :func:`calculate_daily_stats`.
        anomalies: Output from :func:`detect_anomalies`.
        out_dir:   Directory where CSVs will be written (created if missing).
    """
    out_path = Path(out_dir)
    out_path.mkdir(parents=True, exist_ok=True)

    cleaned.to_csv(out_path / "cleaned_data.csv", index=False)
    daily_stats.to_csv(out_path / "summary_statistics.csv", index=False)
    anomalies.to_csv(out_path / "anomalies.csv", index=False)


################################################################################
# 6. Main orchestration
################################################################################


DATASETS = ("data_group_1.csv", "data_group_2.csv", "data_group_3.csv")


def main() -> None:
    """
    End-to-end execution entry point (CLI friendly).
    """
    print("Loading raw CSV files ...")
    raw = load_and_concat_data(DATASETS)

    print("Cleaning data ...")
    clean = clean_data(raw)

    print("Computing daily statistics ...")
    stats = calculate_daily_stats(clean)

    print("Detecting anomalies ...")
    anomalies = detect_anomalies(clean)

    print("Saving outputs ...")
    save_outputs(cleaned=clean, daily_stats=stats, anomalies=anomalies)

    print("Pipeline finished – files written to ./output")


if __name__ == "__main__":
    main()
