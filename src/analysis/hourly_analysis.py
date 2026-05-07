import numpy as np
import os
import pandas as pd


"""
Hourly operational performance analysis.

Focus:
- Throughput vs downtime correlation across production hours
- Zero-production window detection
- Hourly efficiency decay patterns
- Throughput volatility and cross-table consistency validation
"""


# =========================================================
# HOURLY EFFICIENCY SUMMARY
# =========================================================
def analyze_hourly_efficiency(featured_dir: str, output_dir: str):
    """
    Produces a statistical summary of hourly efficiency values
    and identifies zero-operation windows.
    """

    os.makedirs(output_dir, exist_ok=True)

    input_path = os.path.join(featured_dir, "hourly_features.csv")
    df = pd.read_csv(input_path, parse_dates=["date", "timestamp_start", "timestamp_end"])

    summary = pd.DataFrame({
        "metric": [
            "mean_efficiency",
            "median_efficiency",
            "min_efficiency",
            "max_efficiency",
            "efficiency_std",
            "zero_operation_hours",
            "zero_operation_share",
        ],
        "value": [
            df["efficiency"].mean(),
            df["efficiency"].median(),
            df["efficiency"].min(),
            df["efficiency"].max(),
            df["efficiency"].std(),
            df["zero_operation_flag"].sum(),
            df["zero_operation_flag"].mean(),
        ]
    })

    output_path = os.path.join(output_dir, "hourly_efficiency_summary.csv")
    summary.to_csv(output_path, index=False)

    print("\n--- Hourly Efficiency Summary ---")
    print(summary)
    print(f"Saved: {output_path}")


# =========================================================
# THROUGHPUT VS DOWNTIME CORRELATION
# =========================================================
def analyze_throughput_vs_downtime(featured_dir: str, output_dir: str):
    """
    Computes the correlation between hourly throughput and downtime ratio.
    Identifies high-downtime / low-throughput windows.
    """

    os.makedirs(output_dir, exist_ok=True)

    input_path = os.path.join(featured_dir, "hourly_features.csv")
    df = pd.read_csv(input_path, parse_dates=["timestamp_start"])

    analysis_df = df[
        ["timestamp_start", "throughput_per_hour", "downtime_ratio", "production_gallons"]
    ].dropna()

    correlation = analysis_df["throughput_per_hour"].corr(analysis_df["downtime_ratio"])

    threshold_75 = analysis_df["downtime_ratio"].quantile(0.75)
    high_downtime_df = analysis_df[analysis_df["downtime_ratio"] >= threshold_75]

    summary = pd.DataFrame({
        "metric": [
            "throughput_downtime_correlation",
            "mean_throughput_per_hour",
            "median_throughput_per_hour",
            "throughput_std",
            "high_downtime_hour_count",
            "high_downtime_threshold_ratio",
        ],
        "value": [
            correlation,
            analysis_df["throughput_per_hour"].mean(),
            analysis_df["throughput_per_hour"].median(),
            analysis_df["throughput_per_hour"].std(),
            len(high_downtime_df),
            threshold_75,
        ]
    })

    output_path = os.path.join(output_dir, "throughput_downtime_summary.csv")
    summary.to_csv(output_path, index=False)

    print("\n--- Throughput vs Downtime Analysis ---")
    print(summary)
    print(f"Saved: {output_path}")


# =========================================================
# HOURLY DOWNTIME DENSITY BY HOUR-OF-DAY
# =========================================================
def analyze_hourly_downtime_density(featured_dir: str, output_dir: str):
    """
    Aggregates downtime ratio by hour-of-day to reveal
    intraday downtime density patterns.
    """

    os.makedirs(output_dir, exist_ok=True)

    input_path = os.path.join(featured_dir, "hourly_features.csv")
    df = pd.read_csv(input_path, parse_dates=["timestamp_start"])

    density_df = (
        df.groupby("hour")
        .agg(
            mean_downtime_ratio=("downtime_ratio", "mean"),
            mean_efficiency=("efficiency", "mean"),
            mean_throughput=("throughput_per_hour", "mean"),
            total_hours=("hour", "count"),
        )
        .reset_index()
        .sort_values("hour")
    )

    output_path = os.path.join(output_dir, "hourly_downtime_density.csv")
    density_df.to_csv(output_path, index=False)

    print("\n--- Hourly Downtime Density by Hour-of-Day ---")
    print(density_df)
    print(f"Saved: {output_path}")


# =========================================================
# PIPELINE
# =========================================================
def run_hourly_analysis_pipeline(featured_dir: str, output_dir: str):

    print("\nStarting hourly operational analysis...")

    analyze_hourly_efficiency(featured_dir, output_dir)
    analyze_throughput_vs_downtime(featured_dir, output_dir)
    analyze_hourly_downtime_density(featured_dir, output_dir)

    print("Hourly analysis pipeline completed successfully.")
