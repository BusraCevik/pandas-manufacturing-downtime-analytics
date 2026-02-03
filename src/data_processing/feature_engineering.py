import os
import pandas as pd

def build_downtime_features(
        cleaned_dir: str,
        featured_dir: str,
        burst_treshold_sec: int = 300
):
    """
      Build event-level downtime features from downtime_cleaned.csv

      Parameters
      ----------
      cleaned_dir : str
          Path to cleaned data directory
      featured_dir : str
          Path to featured data directory
      burst_threshold_sec : int
          Max gap (seconds) to consider downtime events as clustered
      """

    os.makedirs(featured_dir, exist_ok=True)

    input_path = os.path.join(cleaned_dir, "downtime_cleaned.csv")
    output_path = os.path.join(featured_dir, "downtime_features.csv")

    # -----------------------------
    # Load cleaned downtime data
    # -----------------------------

    df = pd.read_csv(
        input_path,
        parse_dates= ["date", "downtime_start_ts", "downtime_end_ts"]
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

    df.loc[df["downtime_duration_sec"]<0, "downtime_duration_sec"] = pd.NA

    # -----------------------------
    # Temporal positioning features
    # -----------------------------

    df["downtime_hour"] = df["downtime_start_ts"].dt.hour
    df["downtime_weekday"] = df["downtime_start_ts"].dt.weekday

    # -----------------------------
    # Sequential behavior features
    # -----------------------------

    df["prev_downtime_end_ts"] = df["downtime_end_ts"].shift(1)

    df["gap_from_prev_sec"] = (
        df["downtime_start_ts"] - df["prev_downtime_end_ts"]
    ).dt.total_seconds()

    # Recovery time (same thing, semantically clearer)
    df["recovery_time_sec"] = df["gap_from_prev_sec"]

    #Burst / clustered downtime indicator
    df["is_burst"] = df["gap_from_prev_sec"] > burst_treshold_sec

    #First event has no previous reference
    df.loc[df.index == 0, ["gap_from_prev_sec", "recovery_time_sec", "is_burst"]]

    # -----------------------------
    # Save featured dataset
    # -----------------------------
    df.to_csv(output_path, index=False)

    print("Downtime feature engineering completed.")
    print(f"Output saved to: {output_path}")

