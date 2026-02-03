import os
import pandas as pd


def build_downtime_features(
    cleaned_dir: str,
    featured_dir: str,
    burst_threshold_sec: int = 300
):
    """
    Build event-level downtime features from downtime_cleaned.csv
    """

    os.makedirs(featured_dir, exist_ok=True)

    input_path = os.path.join(cleaned_dir, "downtime_cleaned.csv")
    output_path = os.path.join(featured_dir, "downtime_features.csv")

    # -----------------------------
    # Load cleaned downtime data
    # -----------------------------
    df = pd.read_csv(
        input_path,
        parse_dates=["date", "downtime_start_ts", "downtime_end_ts"]
    )

    # -----------------------------
    # Sort by time (critical!)
    # -----------------------------
    df = df.sort_values("downtime_start_ts").reset_index(drop=True)

    # -----------------------------
    # Downtime duration (ground truth)
    # -----------------------------
    df["downtime_duration_sec"] = (
        df["downtime_end_ts"] - df["downtime_start_ts"]
    ).dt.total_seconds()

    # Guard against corrupted records
    df.loc[df["downtime_duration_sec"] < 0, "downtime_duration_sec"] = pd.NA

    # -----------------------------
    # Temporal positioning features
    # -----------------------------
    df["downtime_hour"] = df["downtime_start_ts"].dt.hour
    df["downtime_weekday"] = df["downtime_start_ts"].dt.dayofweek

    # -----------------------------
    # Sequential behavior features
    # -----------------------------
    df["prev_downtime_end_ts"] = df["downtime_end_ts"].shift(1)

    df["gap_from_prev_sec"] = (
        df["downtime_start_ts"] - df["prev_downtime_end_ts"]
    ).dt.total_seconds()

    # Recovery time
    df["recovery_time_sec"] = df["gap_from_prev_sec"]

    # Burst / clustered downtime indicator
    df["is_burst"] = df["gap_from_prev_sec"] < burst_threshold_sec

    # First event has no previous reference
    df.loc[
        df.index == 0,
        ["gap_from_prev_sec", "recovery_time_sec", "is_burst"]
    ] = pd.NA

    # -----------------------------
    # Save featured dataset
    # -----------------------------
    df.to_csv(output_path, index=False)

    print("Downtime feature engineering completed.")
    print(f"Output saved to: {output_path}")


def build_hourly_features(
    cleaned_dir: str,
    featured_dir: str
):
    """
    Build hourly-level operational and throughput features
    from hourly_cleaned.csv and processed_hourly_cleaned.csv
    """

    os.makedirs(featured_dir, exist_ok=True)

    input_path = os.path.join(cleaned_dir, "hourly_cleaned.csv")
    processed_input_path = os.path.join(cleaned_dir, "processed_hourly_cleaned.csv")
    output_path = os.path.join(featured_dir, "hourly_features.csv")

    # -----------------------------
    # Load cleaned data
    # -----------------------------
    hourly_df = pd.read_csv(
        input_path,
        parse_dates=["date", "timestamp_start", "timestamp_end"]
    )

    processed_df = pd.read_csv(
        processed_input_path,
        parse_dates=["date", "timestamp_start", "timestamp_end"]
    )

    # -----------------------------
    # Downtime ratio (core KPI)
    # -----------------------------
    hourly_df["downtime_ratio"] = (
        hourly_df["downtime"] / hourly_df["monitored_time"]
    )

    # -----------------------------
    # Zero operation flag
    # -----------------------------
    hourly_df["zero_operation_flag"] = hourly_df["operation_time"] == 0

    # -----------------------------
    # Efficiency loss
    # -----------------------------
    if "efficiency" in hourly_df.columns:
        hourly_df["efficiency_loss"] = 100 - hourly_df["efficiency"]

    # -----------------------------
    # Throughput per hour
    # -----------------------------
    processed_df["hour_duration_sec"] = (
        processed_df["timestamp_end"] - processed_df["timestamp_start"]
    ).dt.total_seconds()

    processed_df["throughput_per_hour"] = (
        processed_df["production_volume"] /
        processed_df["hour_duration_sec"]
    ) * 3600

    # -----------------------------
    # Join throughput into hourly ops
    # -----------------------------
    hourly_df = hourly_df.merge(
        processed_df[
            ["timestamp_start", "throughput_per_hour", "production_volume"]
        ],
        on="timestamp_start",
        how="left"
    )

    # -----------------------------
    # Hour-based context features
    # -----------------------------
    hourly_df["hour"] = hourly_df["timestamp_start"].dt.hour
    hourly_df["weekday"] = hourly_df["timestamp_start"].dt.dayofweek

    # -----------------------------
    # Save featured dataset
    # -----------------------------
    hourly_df.to_csv(output_path, index=False)

    print("Hourly feature engineering completed.")
    print(f"Output saved to: {output_path}")
