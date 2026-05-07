import os
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.colors import LinearSegmentedColormap


# -----------------------------
# Theme
# -----------------------------
THEME = {
    "primary": "#5FA8A8",
    "light":   "#9ED6D6",
    "dark":    "#3E7C7C",
    "grid":    "#E6F2F2",
    "accent":  "#F2B6C6",
}

HEATMAP_CMAP = LinearSegmentedColormap.from_list(
    "theme_heatmap",
    [
        "#E6F2F2",
        "#9ED6D6",
        "#5FA8A8",
        "#3E7C7C",
    ]
)

FIG_SIZE = (8, 5)


# -----------------------------
# Shared Plot Helpers
# -----------------------------
def _save_bar_plot(df, x_col, y_col, title, x_label, y_label, save_path, rotate_x=0):

    plt.figure(figsize=FIG_SIZE)

    bars = plt.bar(
        range(len(df)),
        df[y_col],
        color=THEME["primary"],
        edgecolor=THEME["dark"],
        width=0.5
    )

    x_values = df[x_col].astype(str)

    plt.xticks(
        ticks=range(len(x_values)),
        labels=x_values,
        rotation=rotate_x
    )

    plt.title(title, color=THEME["dark"])
    plt.xlabel(x_label, color=THEME["dark"])
    plt.ylabel(y_label, color=THEME["dark"])
    plt.grid(axis="y", color=THEME["grid"])
    plt.xticks(color=THEME["dark"])
    plt.yticks(color=THEME["dark"])

    for bar in bars:
        height = bar.get_height()
        x_center = bar.get_x() + bar.get_width() / 2
        plt.text(
            x_center,
            height,
            f"{height:,.2f}",
            ha="center",
            va="bottom",
            fontsize=8
        )

    plt.tight_layout()
    plt.savefig(save_path, dpi=300)
    plt.close()


def _save_line_plot(df, x_col, y_col, title, x_label, y_label, save_path):

    plt.figure(figsize=FIG_SIZE)

    plt.plot(
        df[x_col],
        df[y_col],
        marker="o",
        linewidth=2,
        color=THEME["primary"]
    )

    plt.title(title, color=THEME["dark"])
    plt.xlabel(x_label, color=THEME["dark"])
    plt.ylabel(y_label, color=THEME["dark"])
    plt.grid(color=THEME["grid"])
    plt.xticks(rotation=45, color=THEME["dark"])
    plt.yticks(color=THEME["dark"])
    plt.tight_layout()
    plt.savefig(save_path, dpi=300)
    plt.close()


def _save_scatter_plot(df, x_col, y_col, title, x_label, y_label, save_path):

    plt.figure(figsize=FIG_SIZE)

    plt.scatter(
        df[x_col],
        df[y_col],
        color=THEME["primary"],
        edgecolors=THEME["dark"],
        alpha=0.7,
        linewidths=0.5
    )

    plt.title(title, color=THEME["dark"])
    plt.xlabel(x_label, color=THEME["dark"])
    plt.ylabel(y_label, color=THEME["dark"])
    plt.grid(color=THEME["grid"])
    plt.xticks(color=THEME["dark"])
    plt.yticks(color=THEME["dark"])
    plt.tight_layout()
    plt.savefig(save_path, dpi=300)
    plt.close()


def _save_heatmap(matrix_df, title, x_label, y_label, save_path):

    plt.figure(figsize=(10, 6))

    plt.imshow(
        matrix_df.values,
        aspect="auto",
        cmap=HEATMAP_CMAP
    )

    plt.title(title, color=THEME["dark"])
    plt.xlabel(x_label, color=THEME["dark"])
    plt.ylabel(y_label, color=THEME["dark"])

    plt.xticks(
        ticks=range(len(matrix_df.columns)),
        labels=matrix_df.columns.astype(str),
        rotation=45,
        ha="right",
        color=THEME["dark"]
    )
    plt.yticks(
        ticks=range(len(matrix_df.index)),
        labels=matrix_df.index.astype(str),
        color=THEME["dark"]
    )

    plt.colorbar()
    plt.tight_layout()
    plt.savefig(save_path, dpi=300)
    plt.close()


# -----------------------------
# Individual Plot Functions
# -----------------------------

# ---- Downtime: event duration distribution ----
def _plot_downtime_event_distribution(tables_dir, fig_dir):

    df = pd.read_csv(os.path.join(tables_dir, "downtime_duration_summary.csv"))

    _save_bar_plot(
        df=df,
        x_col="metric",
        y_col="value",
        title="Downtime Duration Statistics",
        x_label="Metric",
        y_label="Seconds",
        save_path=os.path.join(fig_dir, "downtime_event_distribution.png"),
        rotate_x=15
    )


# ---- Downtime: hourly density heatmap (hour-of-day x weekday) ----
def _plot_downtime_density_heatmap(featured_dir, fig_dir):

    df = pd.read_csv(
        os.path.join(featured_dir, "downtime_features.csv"),
        parse_dates=["downtime_start_ts"]
    )

    df["hour"] = df["downtime_start_ts"].dt.hour
    df["weekday"] = df["downtime_start_ts"].dt.dayofweek

    pivot = (
        df.groupby(["weekday", "hour"])["downtime_duration_sec"]
        .sum()
        .unstack(fill_value=0)
    )

    pivot.index = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"][: len(pivot)]

    _save_heatmap(
        matrix_df=pivot,
        title="Total Downtime by Weekday & Hour-of-Day (seconds)",
        x_label="Hour of Day",
        y_label="Weekday",
        save_path=os.path.join(fig_dir, "downtime_density_heatmap.png")
    )


# ---- Hourly: efficiency trend ----
def _plot_hourly_efficiency_trend(tables_dir, fig_dir):

    df = pd.read_csv(os.path.join(tables_dir, "hourly_downtime_density.csv"))

    _save_bar_plot(
        df=df,
        x_col="hour",
        y_col="mean_efficiency",
        title="Mean Hourly Efficiency by Hour-of-Day",
        x_label="Hour of Day",
        y_label="Mean Efficiency",
        save_path=os.path.join(fig_dir, "hourly_efficiency_trend.png")
    )


# ---- Hourly: throughput vs downtime scatter ----
def _plot_throughput_vs_downtime(featured_dir, fig_dir):

    df = pd.read_csv(os.path.join(featured_dir, "hourly_features.csv"))
    df = df[["throughput_per_hour", "downtime_ratio"]].dropna()

    _save_scatter_plot(
        df=df,
        x_col="downtime_ratio",
        y_col="throughput_per_hour",
        title="Throughput vs Downtime Ratio (per Hour)",
        x_label="Downtime Ratio",
        y_label="Throughput (gallons/hour)",
        save_path=os.path.join(fig_dir, "throughput_vs_downtime.png")
    )


# ---- Daily: efficiency trend ----
def _plot_daily_efficiency_trend(featured_dir, fig_dir):

    df = pd.read_csv(os.path.join(featured_dir, "daily_features.csv"))
    df = df.sort_values("date")

    _save_line_plot(
        df=df,
        x_col="date",
        y_col="efficiency",
        title="Daily Efficiency Trend",
        x_label="Date",
        y_label="Efficiency",
        save_path=os.path.join(fig_dir, "daily_efficiency_trend.png")
    )


# ---- Daily: pause ratio distribution ----
def _plot_pause_ratio_distribution(tables_dir, fig_dir):

    df = pd.read_csv(os.path.join(tables_dir, "pause_ratio_summary.csv"))

    _save_bar_plot(
        df=df,
        x_col="metric",
        y_col="value",
        title="Pause Ratio Distribution (Daily)",
        x_label="Metric",
        y_label="Ratio",
        save_path=os.path.join(fig_dir, "pause_ratio_distribution.png"),
        rotate_x=15
    )


# ---- Consistency: event vs hour downtime diff ----
def _plot_consistency_validation(featured_dir, fig_dir):

    df = pd.read_csv(
        os.path.join(featured_dir, "event_hour_reconciliation.csv"),
        parse_dates=["timestamp_start"]
    )

    df_sample = df[["timestamp_start", "event_vs_hour_downtime_diff_sec"]].dropna()
    df_sample = df_sample.sort_values("timestamp_start").reset_index(drop=True)
    df_sample["index"] = df_sample.index

    _save_scatter_plot(
        df=df_sample,
        x_col="index",
        y_col="event_vs_hour_downtime_diff_sec",
        title="Event-Level vs Hourly Downtime Reconciliation Gap (sec)",
        x_label="Hour Record Index",
        y_label="Difference (sec)",
        save_path=os.path.join(fig_dir, "consistency_validation.png")
    )


# -----------------------------
# Public Runner
# -----------------------------
def generate_visualizations(featured_dir: str, tables_dir: str, fig_dir: str):

    os.makedirs(fig_dir, exist_ok=True)

    _plot_downtime_event_distribution(tables_dir, fig_dir)
    _plot_downtime_density_heatmap(featured_dir, fig_dir)
    _plot_hourly_efficiency_trend(tables_dir, fig_dir)
    _plot_throughput_vs_downtime(featured_dir, fig_dir)
    _plot_daily_efficiency_trend(featured_dir, fig_dir)
    _plot_pause_ratio_distribution(tables_dir, fig_dir)
    _plot_consistency_validation(featured_dir, fig_dir)

    print("Visualization files created in:", fig_dir)
