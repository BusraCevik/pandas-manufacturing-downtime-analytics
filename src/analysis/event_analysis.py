import os
import pandas as pd


"""
Event-level downtime analysis:
- Analyze how downtime durations are distributed
- Identify long-tail (Pareto-like) behavior in downtime events
- Produce clean analytical artifacts for visualization
"""


def analyze_downtime_duration(featured_dir: str, output_dir: str):
    """
    Analyzes downtime duration distribution and produces:
    A statistical summary table
    A clean distribution dataset for visualization
    """

    os.makedirs(output_dir, exist_ok=True)

    input_path = os.path.join(featured_dir, "downtime_features.csv")
    df = pd.read_csv(input_path)

    duration_col = "downtime_duration_sec"

    # ---------------------------------------------------------
    # Core statistics
    # ---------------------------------------------------------
    median_duration = df[duration_col].median()
    mean_duration = df[duration_col].mean()
    threshold_95 = df[duration_col].quantile(0.95)

    long_event_share = (
        df[df[duration_col] >= threshold_95][duration_col].sum()
        / df[duration_col].sum()
    )

    # ---------------------------------------------------------
    # Summary table
    # ---------------------------------------------------------
    summary_df = pd.DataFrame({
        "metric": [
            "median_duration_sec",
            "mean_duration_sec",
            "95th_percentile_duration_sec",
            "long_event_downtime_share"
        ],
        "value": [
            median_duration,
            mean_duration,
            threshold_95,
            long_event_share
        ]
    })

    summary_output_path = os.path.join(
        output_dir,
        "downtime_duration_summary.csv"
    )
    summary_df.to_csv(summary_output_path, index=False)

    # ---------------------------------------------------------
    # Distribution dataset
    # ---------------------------------------------------------
    distribution_df = df[
        [
            "downtime_duration_sec",
            "is_burst"
        ]
    ].copy()

    distribution_output_path = os.path.join(
        output_dir,
        "downtime_duration_distribution.csv"
    )
    distribution_df.to_csv(distribution_output_path, index=False)

    print("\n--- Downtime Duration Analysis Completed ---")
    print(f"Median duration (sec): {median_duration:.2f}")
    print(f"95th percentile (sec): {threshold_95:.2f}")
    print(f"Top 5% downtime share: {long_event_share:.2%}")
    print(f"Saved: {summary_output_path}")
    print(f"Saved: {distribution_output_path}")

def analyze_burst_behavior(featured_dir: str, output_dir: str):
    """
    Analyzes burst vs non-burst downtime behavior.

    Produces a summary table showing:
    - event count
    - total downtime
    - downtime share
    """

    os.makedirs(output_dir, exist_ok=True)

    input_path = os.path.join(featured_dir, "downtime_features.csv")
    df = pd.read_csv(input_path)

    duration_col = "downtime_duration_sec"

    burst_summary = (
        df
        .groupby("is_burst", dropna=False)
        .agg(
            event_count=("is_burst", "count"),
            total_downtime_sec=(duration_col, "sum")
        )
        .reset_index()
    )

    total_downtime = burst_summary["total_downtime_sec"].sum()

    burst_summary["downtime_share"] = (
        burst_summary["total_downtime_sec"] / total_downtime
    )

    output_path = os.path.join(
        output_dir,
        "downtime_burst_summary.csv"
    )

    burst_summary.to_csv(output_path, index=False)

    print("\n--- Burst Analysis Completed ---")
    print(burst_summary)
    print(f"Saved: {output_path}")


def run_event_analysis_pipeline(featured_dir: str, output_dir: str):


    print("\nStarting event-level downtime analysis...")

    analyze_downtime_duration(
        featured_dir=featured_dir,
        output_dir=output_dir
    )
    analyze_burst_behavior(
        featured_dir=featured_dir,
        output_dir=output_dir
    )

