import os
import pandas as pd


# Excel sheet mapping
SHEETS = {
    "downtime": "downtime_event_log",
    "hourly": "hourly_operation_breakdown",
    "daily": "daily_operation_summary",
    "processed": "processed_hourly"
}


def _extract_clock(value):
    if pd.isna(value):
        return pd.NaT

    # Already datetime.time
    if isinstance(value, pd.Timestamp):
        return value.time()

    if hasattr(value, "hour"):
        return value

    # Excel serial float (fraction of day)
    try:
        seconds = float(value) * 24 * 60 * 60
        return pd.to_datetime(seconds, unit="s").time()
    except Exception:
        return pd.NaT


def prepare_data(raw_excel_path: str, cleaned_dir: str):

    os.makedirs(cleaned_dir, exist_ok=True)

    # Output paths
    cleaned_paths = {
        "downtime": os.path.join(cleaned_dir, "downtime_cleaned.csv"),
        "hourly": os.path.join(cleaned_dir, "hourly_cleaned.csv"),
        "daily": os.path.join(cleaned_dir, "daily_cleaned.csv"),
        "processed": os.path.join(cleaned_dir, "processed_hourly_cleaned.csv"),
    }

    # -----------------------------
    # Load raw sheets
    # -----------------------------
    downtime_df = pd.read_excel(raw_excel_path, sheet_name=SHEETS["downtime"])
    hourly_df = pd.read_excel(raw_excel_path, sheet_name=SHEETS["hourly"])
    daily_df = pd.read_excel(raw_excel_path, sheet_name=SHEETS["daily"])
    processed_df = pd.read_excel(raw_excel_path, sheet_name=SHEETS["processed"])

    # =========================================================
    # processed_hourly — timestamp generation
    # =========================================================

    processed_df["date"] = pd.to_datetime(processed_df["date"], errors="coerce")

    processed_df["hour_start"] = processed_df["hour_start"].astype(int)
    processed_df["hour_end"] = processed_df["hour_end"].astype(int)

    processed_df["timestamp_start"] = (
        processed_df["date"] +
        pd.to_timedelta(processed_df["hour_start"], unit="h")
    )

    processed_df["timestamp_end"] = (
        processed_df["date"] +
        pd.to_timedelta(processed_df["hour_end"], unit="h")
    )

    print("processed_hourly timestamps preview:")
    print(
        processed_df[
            ["date", "hour_start", "hour_end", "timestamp_start", "timestamp_end"]
        ].head()
    )

    # =========================================================
    # daily_operation_summary — normalize clock and timestamps
    # =========================================================

    daily_df["date"] = pd.to_datetime(daily_df["date"], errors="coerce")

    daily_df["start_clock"] = daily_df["production_start_time"].apply(_extract_clock)
    daily_df["end_clock"] = daily_df["production_end_time"].apply(_extract_clock)

    daily_df["production_start_ts"] = daily_df.apply(
        lambda row: (
            pd.Timestamp.combine(row["date"], row["start_clock"])
            if pd.notna(row["start_clock"]) else pd.NaT
        ),
        axis=1
    )

    daily_df["production_end_ts"] = daily_df.apply(
        lambda row: (
            pd.Timestamp.combine(row["date"], row["end_clock"])
            if pd.notna(row["end_clock"]) else pd.NaT
        ),
        axis=1
    )
    daily_df["production_start_ts"] = daily_df["production_start_ts"].dt.floor("s")
    daily_df["production_end_ts"] = daily_df["production_end_ts"].dt.floor("s")

    print("daily_operation_summary timestamps preview:")
    print(
        daily_df[
            ["date", "start_clock", "production_start_ts",
             "end_clock", "production_end_ts"]
        ].head()
    )

    # =========================================================
    # Save cleaned outputs

    downtime_df.to_csv(cleaned_paths["downtime"], index=False)
    hourly_df.to_csv(cleaned_paths["hourly"], index=False)
    daily_df.to_csv(cleaned_paths["daily"], index=False)
    processed_df.to_csv(cleaned_paths["processed"], index=False)

    print("✅ Data preparation completed.")
