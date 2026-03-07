import os
import pandas as pd


"""
Daily operational performance analysis.

Focus:
- Efficiency behavior across production days
- Operational pause patterns
- Identifying best vs worst performing days
- Measuring stability of daily operations
"""


# =========================================================
# DAILY EFFICIENCY SUMMARY
# =========================================================
def analyze_daily_efficiency(featured_dir: str, output_dir: str):

    os.makedirs(output_dir, exist_ok=True)

    input_path = os.path.join(featured_dir, "daily_features.csv")
    df = pd.read_csv(input_path, parse_dates=["date"])

    summary = pd.DataFrame({
        "metric": [
            "mean_efficiency",
            "median_efficiency",
            "min_efficiency",
            "max_efficiency",
            "efficiency_std"
        ],
        "value": [
            df["efficiency"].mean(),
            df["efficiency"].median(),
            df["efficiency"].min(),
            df["efficiency"].max(),
            df["efficiency"].std()
        ]
    })

    output_path = os.path.join(
        output_dir,
        "daily_efficiency_summary.csv"
    )

    summary.to_csv(output_path, index=False)

    print("\n--- Daily Efficiency Summary ---")
    print(summary)
    print(f"Saved: {output_path}")


# =========================================================
# PAUSE RATIO DISTRIBUTION
# =========================================================
def analyze_pause_behavior(featured_dir: str, output_dir: str):

    os.makedirs(output_dir, exist_ok=True)

    input_path = os.path.join(featured_dir, "daily_features.csv")
    df = pd.read_csv(input_path, parse_dates=["date"])

    pause_stats = pd.DataFrame({
        "metric": [
            "mean_pause_ratio",
            "median_pause_ratio",
            "max_pause_ratio",
            "pause_ratio_std"
        ],
        "value": [
            df["pause_ratio"].mean(),
            df["pause_ratio"].median(),
            df["pause_ratio"].max(),
            df["pause_ratio"].std()
        ]
    })

    output_path = os.path.join(
        output_dir,
        "pause_ratio_summary.csv"
    )

    pause_stats.to_csv(output_path, index=False)

    print("\n--- Pause Behavior Analysis ---")
    print(pause_stats)
    print(f"Saved: {output_path}")


# =========================================================
# BEST VS WORST OPERATIONAL DAYS
# =========================================================
def analyze_operational_extremes(featured_dir: str, output_dir: str):

    os.makedirs(output_dir, exist_ok=True)

    input_path = os.path.join(featured_dir, "daily_features.csv")
    df = pd.read_csv(input_path, parse_dates=["date"])

    best_day = df.loc[df["efficiency"].idxmax()]
    worst_day = df.loc[df["efficiency"].idxmin()]

    extremes = pd.DataFrame({
        "type": ["best_day", "worst_day"],
        "date": [best_day["date"], worst_day["date"]],
        "efficiency": [best_day["efficiency"], worst_day["efficiency"]],
        "pause_ratio": [best_day["pause_ratio"], worst_day["pause_ratio"]],
        "operation_time": [
            best_day["operation_time_dec"],
            worst_day["operation_time_dec"]
        ]
    })

    output_path = os.path.join(
        output_dir,
        "daily_operational_extremes.csv"
    )

    extremes.to_csv(output_path, index=False)

    print("\n--- Operational Extremes ---")
    print(extremes)
    print(f"Saved: {output_path}")


# =========================================================
# OPERATIONAL STABILITY
# =========================================================
def analyze_operational_stability(featured_dir: str, output_dir: str):

    os.makedirs(output_dir, exist_ok=True)

    input_path = os.path.join(featured_dir, "daily_features.csv")
    df = pd.read_csv(input_path, parse_dates=["date"])

    stability = pd.DataFrame({
        "metric": [
            "efficiency_volatility",
            "pause_ratio_volatility"
        ],
        "value": [
            df["efficiency"].std(),
            df["pause_ratio"].std()
        ]
    })

    output_path = os.path.join(
        output_dir,
        "operational_stability_metrics.csv"
    )

    stability.to_csv(output_path, index=False)

    print("\n--- Operational Stability ---")
    print(stability)
    print(f"Saved: {output_path}")


# =========================================================
# PIPELINE
# =========================================================
def run_daily_analysis_pipeline(featured_dir: str, output_dir: str):

    print("\nStarting daily operational analysis...")

    analyze_daily_efficiency(featured_dir, output_dir)
    analyze_pause_behavior(featured_dir, output_dir)
    analyze_operational_extremes(featured_dir, output_dir)
    analyze_operational_stability(featured_dir, output_dir)

    print("\nDaily analysis pipeline completed.")