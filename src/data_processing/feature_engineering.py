import os
import pandas as pd


# =========================================================
# EVENT-LEVEL FEATURES
# =========================================================
def build_downtime_features(cleaned_dir: str, featured_dir: str):
    os.makedirs(featured_dir, exist_ok=True)

    input_path = os.path.join(cleaned_dir, "downtime_cleaned.csv")
    output_path = os.path.join(featured_dir, "downtime_features.csv")

    df = pd.read_csv(
        input_path,
        parse_dates=["date", "downtime_start_ts", "downtime_end_ts"]
    )

    df = df.sort_values("downtime_start_ts").reset_index(drop=True)

    df["downtime_duration_sec"] = (
        df["downtime_end_ts"] - df["downtime_start_ts"]
    ).dt.total_seconds()

    df["downtime_hour"] = df["downtime_start_ts"].dt.hour
    df["downtime_weekday"] = df["downtime_start_ts"].dt.dayofweek

    df["prev_downtime_end_ts"] = df["downtime_end_ts"].shift(1)

    df["gap_from_prev_sec"] = (
        df["downtime_start_ts"] - df["prev_downtime_end_ts"]
    ).dt.total_seconds()

    df["recovery_time_sec"] = df["gap_from_prev_sec"]

    df["is_burst"] = (df["gap_from_prev_sec"] < 300).astype("boolean")

    df.loc[
        df.index == 0,
        ["gap_from_prev_sec", "recovery_time_sec", "is_burst"]
    ] = pd.NA

    df.to_csv(output_path, index=False)
    print("downtime_features.csv created")


# =========================================================
# HOURLY FEATURES
# =========================================================
def build_hourly_features(cleaned_dir: str, featured_dir: str):
    os.makedirs(featured_dir, exist_ok=True)

    hourly_path = os.path.join(cleaned_dir, "hourly_cleaned.csv")
    processed_path = os.path.join(cleaned_dir, "processed_hourly_cleaned.csv")
    output_path = os.path.join(featured_dir, "hourly_features.csv")

    hourly_df = pd.read_csv(
        hourly_path,
        parse_dates=["date", "timestamp_start", "timestamp_end"]
    )

    processed_df = pd.read_csv(
        processed_path,
        parse_dates=["date", "timestamp_start", "timestamp_end"]
    )

    # Downtime ratio expresses the share of the monitored hour lost to downtime
    hourly_df["downtime_ratio"] = (
            hourly_df["downtime_h"] / hourly_df["monitored_time_h"]
    ).where(hourly_df["monitored_time_h"] > 0)

    # Efficiency loss complements downtime_ratio by highlighting
    # how much productive capacity was lost within the hour
    hourly_df["efficiency_loss"] = 1 - hourly_df["efficiency"]

    hourly_df["zero_operation_flag"] = hourly_df["operation_time_h"] == 0


    # Throughput (processed_hourly_cleaned)
    processed_df["hour_duration_sec"] = (
        processed_df["timestamp_end"] - processed_df["timestamp_start"]
    ).dt.total_seconds()

    processed_df["throughput_per_hour"] = (
        processed_df["production_gallons"] /
        processed_df["hour_duration_sec"]
    ) * 3600

    hourly_df = hourly_df.merge(
        processed_df[
            ["timestamp_start", "throughput_per_hour", "production_gallons"]
        ],
        on="timestamp_start",
        how="left"
    )

    hourly_df["hour"] = hourly_df["timestamp_start"].dt.hour
    hourly_df["weekday"] = hourly_df["timestamp_start"].dt.dayofweek

    hourly_df.to_csv(output_path, index=False)
    print("hourly_features.csv created")


# =========================================================
# DAILY FEATURES
# =========================================================
def build_daily_features(cleaned_dir: str, featured_dir: str):
    os.makedirs(featured_dir, exist_ok=True)

    input_path = os.path.join(cleaned_dir, "daily_cleaned.csv")
    output_path = os.path.join(featured_dir, "daily_features.csv")

    df = pd.read_csv(input_path, parse_dates=["date"])

    # ---------------------------------------------------------
    # Pause ratio represents the proportion of non-operational time.
    # In this dataset, pause_ratio is mathematically equivalent to
    # (1 - efficiency), since:
    #   efficiency = operation_time / monitored_time
    #   pause_ratio = pause_time / monitored_time
    # Therefore, efficiency_loss is intentionally omitted to avoid
    # redundant features and improve interpretability.
    # ---------------------------------------------------------
    df["pause_ratio"] = df["pause_time_dec"] / df["monitored_time_dec"]

    #-> commented later on df["efficiency_loss"] = 1 - df["efficiency"]
    df["operation_pause_balance"] = (
        df["operation_time_dec"] - df["pause_time_dec"]
    )

    df = df.sort_values("date").reset_index(drop=True)
    df["efficiency_rolling_std"] = (
        df["efficiency"].rolling(window=5).std()
    )

    df.to_csv(output_path, index=False)
    print("daily_features.csv created")


# =========================================================
# EVENT → HOUR RECONCILIATION
# =========================================================
def build_event_hour_reconciliation(cleaned_dir: str, featured_dir: str):
    os.makedirs(featured_dir, exist_ok=True)

    downtime_path = os.path.join(cleaned_dir, "downtime_cleaned.csv")
    hourly_path = os.path.join(cleaned_dir, "hourly_cleaned.csv")
    output_path = os.path.join(featured_dir, "event_hour_reconciliation.csv")

    downtime_df = pd.read_csv(
        downtime_path,
        parse_dates=["downtime_start_ts", "downtime_end_ts"]
    )

    hourly_df = pd.read_csv(
        hourly_path,
        parse_dates=["timestamp_start"]
    )

    downtime_df["hour_bucket"] = downtime_df["downtime_start_ts"].dt.floor("h")

    event_hourly = (
        downtime_df
        .assign(duration_sec=lambda x: (
            x["downtime_end_ts"] - x["downtime_start_ts"]
        ).dt.total_seconds())
        .groupby("hour_bucket", as_index=False)["duration_sec"]
        .sum()
        .rename(columns={"hour_bucket": "timestamp_start"})
    )

    merged = hourly_df.merge(
        event_hourly,
        on="timestamp_start",
        how="left"
    )

    merged["duration_sec"] = merged["duration_sec"].fillna(0)
    merged["hourly_downtime_sec"] = merged["downtime_h"] * 3600

    merged["event_vs_hour_downtime_diff_sec"] = (
        merged["hourly_downtime_sec"] - merged["duration_sec"]
    )

    merged.to_csv(output_path, index=False)
    print("event_hour_reconciliation.csv created")


# =========================================================
# HOUR → DAY RECONCILIATION
# =========================================================
def build_hour_day_reconciliation(cleaned_dir: str, featured_dir: str):
    os.makedirs(featured_dir, exist_ok=True)

    hourly_path = os.path.join(cleaned_dir, "hourly_cleaned.csv")
    daily_path = os.path.join(cleaned_dir, "daily_cleaned.csv")
    output_path = os.path.join(featured_dir, "hour_day_reconciliation.csv")

    hourly_df = pd.read_csv(hourly_path, parse_dates=["date"])
    daily_df = pd.read_csv(daily_path, parse_dates=["date"])

    hourly_daily = (
        hourly_df
        .groupby("date", as_index=False)
        .agg(
            hourly_downtime_sum=("downtime_h", "sum"),
            hourly_operation_sum=("operation_time_h", "sum"),
            hourly_efficiency_mean=("efficiency", "mean")
        )
    )

    merged = daily_df.merge(hourly_daily, on="date", how="left")

    merged["hour_vs_day_efficiency_diff"] = (
        merged["efficiency"] - merged["hourly_efficiency_mean"]
    )

    merged.to_csv(output_path, index=False)
    print("hour_day_reconciliation.csv created")


# =========================================================
# PIPELINE
# =========================================================
def run_feature_pipeline(cleaned_dir: str, featured_dir: str):
    print("Starting feature engineering pipeline...")

    build_downtime_features(cleaned_dir, featured_dir)
    build_hourly_features(cleaned_dir, featured_dir)
    build_daily_features(cleaned_dir, featured_dir)
    build_event_hour_reconciliation(cleaned_dir, featured_dir)
    build_hour_day_reconciliation(cleaned_dir, featured_dir)

    print("Feature engineering pipeline completed successfully.")
