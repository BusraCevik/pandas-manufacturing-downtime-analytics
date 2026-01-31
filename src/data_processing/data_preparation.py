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

    # If already pandas Timestamp
    if isinstance(value, pd.Timestamp):
        return value.time()

    # If already datetime.time
    if hasattr(value, "hour") and hasattr(value, "minute"):
        return value

    # Excel serial float (fraction of day)
    try:
        # Excel stores time as fraction of a day
        td = pd.to_timedelta(float(value), unit="D")
        return (pd.Timestamp("1970-01-01") + td).time()
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

    # Safer conversion
    processed_df["hour_start"] = pd.to_numeric(
        processed_df["hour_start"], errors="coerce"
    )
    processed_df["hour_end"] = pd.to_numeric(
        processed_df["hour_end"], errors="coerce"
    )

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
    # daily_operation_summary — normalizing clock and timestamps
    # =========================================================

    daily_df["date"] = pd.to_datetime(daily_df["date"], errors="coerce")

    daily_df["start_clock"] = daily_df["production_start_time"].apply(_extract_clock)
    daily_df["end_clock"] = daily_df["production_end_time"].apply(_extract_clock)

    daily_df["production_start_ts"] = daily_df.apply(
        lambda row: (
            pd.Timestamp.combine(row["date"], row["start_clock"])
            if pd.notna(row["date"]) and pd.notna(row["start_clock"])
            else pd.NaT
        ),
        axis=1
    )

    daily_df["production_end_ts"] = daily_df.apply(
        lambda row: (
            pd.Timestamp.combine(row["date"], row["end_clock"])
            if pd.notna(row["date"]) and pd.notna(row["end_clock"])
            else pd.NaT
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
    # downtime_event_log - normalize clock and timestamps
    # =========================================================

    #date->datetime
    downtime_df["date"] = pd.to_datetime(downtime_df["date"], errors="coerce")

    #start/ende clock normalize
    downtime_df["start_clock"] = downtime_df["downtime_start_time"].apply(_extract_clock)
    downtime_df["end_clock"] = downtime_df["downtime_end_time"].apply(_extract_clock)

    #combine date + clock + timestamp
    downtime_df["downtime_start_ts"] = downtime_df.apply(
        lambda row: (
            pd.Timestamp.combine(row["date"], row["start_clock"])
            if pd.notna(row["date"]) and pd.notna(row["start_clock"])
            else pd.NaT
        ),
        axis=1
    )

    downtime_df["downtime_end_ts"] = downtime_df.apply(
        lambda row: (
            pd.Timestamp.combine(row["date"], row["end_clock"])
            if pd.notna(row["date"]) and pd.notna(row["end_clock"])
            else pd.NaT
        ),
        axis=1
    )
    # floor to seconds (drop milliseconds)
    downtime_df["downtime_start_ts"] = downtime_df["downtime_start_ts"].dt.floor("s")
    downtime_df["downtime_end_ts"] = downtime_df["downtime_end_ts"].dt.floor("s")

    print("downtime_event_log timestamps preview:")
    print(
        downtime_df[
            [
                "date",
                "downtime_start_time",
                "downtime_end_time",
                "downtime_start_ts",
                "downtime_end_ts",
            ]
        ].head()
    )
    # =========================================================
    # hourly_operation_breakdown — normalize timestamps & numerics
    # =========================================================

    # date → datetime
    hourly_df["date"] = pd.to_datetime(hourly_df["date"], errors="coerce")

    # hour_start / hour_end → numeric
    hourly_df["hour_start"] = pd.to_numeric(hourly_df["hour_start"], errors="coerce")
    hourly_df["hour_end"] = pd.to_numeric(hourly_df["hour_end"], errors="coerce")

    # combine date + hour → timestamps
    hourly_df["timestamp_start"] = (
            hourly_df["date"] + pd.to_timedelta(hourly_df["hour_start"], unit="h")
    )
    hourly_df["timestamp_end"] = (
            hourly_df["date"] + pd.to_timedelta(hourly_df["hour_end"], unit="h")
    )

    # floor to seconds (safety)
    hourly_df["timestamp_start"] = hourly_df["timestamp_start"].dt.floor("s")
    hourly_df["timestamp_end"] = hourly_df["timestamp_end"].dt.floor("s")

    # ---- numeric cleanup (decimal comma → dot)
    # Bu kolon isimlerini kendi dosyana göre uyarlayabilirsin
    numeric_cols = [
        "monitored_time",
        "operation_time",
        "downtime",
    ]

    for col in numeric_cols:
        if col in hourly_df.columns:
            hourly_df[col] = (
                hourly_df[col]
                .astype(str)
                .str.replace(",", ".", regex=False)
            )
            hourly_df[col] = pd.to_numeric(hourly_df[col], errors="coerce")

    # efficiency % ise temizle
    if "efficiency" in hourly_df.columns:
        hourly_df["efficiency"] = (
            hourly_df["efficiency"]
            .astype(str)
            .str.replace("%", "", regex=False)
            .str.replace(",", ".", regex=False)
        )
        hourly_df["efficiency"] = pd.to_numeric(hourly_df["efficiency"], errors="coerce")

    print("hourly_operation_breakdown timestamps preview:")
    print(
        hourly_df[
            ["date", "hour_start", "hour_end", "timestamp_start", "timestamp_end"]
        ].head()
    )

    # -----------------------------
    # Save cleaned files
    # -----------------------------
    downtime_df.to_csv(cleaned_paths["downtime"], index=False)
    hourly_df.to_csv(cleaned_paths["hourly"], index=False)
    daily_df.to_csv(cleaned_paths["daily"], index=False)
    processed_df.to_csv(cleaned_paths["processed"], index=False)

    print("Data preparation completed.")
