import os
import pandas as pd
import plotly.graph_objects as go
import plotly.io as pio


# -----------------------------
# Color Theme  (identical to project 05)
# -----------------------------
MAIN_COLOR  = "#5FA8A8"
DARK_COLOR  = "#3E7C7C"
GRID_COLOR  = "#E6F2F2"
BORDER_COLOR = "#000000"

HEATMAP_COLORSCALE = [
    [0.0, "#E6F2F2"],
    [0.3, "#9ED6D6"],
    [0.6, "#5FA8A8"],
    [1.0, "#3E7C7C"],
]


def build_manufacturing_dashboard(
    featured_dir: str,
    tables_dir: str,
    output_html_path: str
):

    os.makedirs(os.path.dirname(output_html_path), exist_ok=True)

    # -----------------------------
    # Load data
    # -----------------------------
    daily_df = pd.read_csv(
        os.path.join(featured_dir, "daily_features.csv"),
        parse_dates=["date"]
    ).sort_values("date")

    hourly_density_df = pd.read_csv(
        os.path.join(tables_dir, "hourly_downtime_density.csv")
    )

    hourly_df = pd.read_csv(
        os.path.join(featured_dir, "hourly_features.csv"),
        parse_dates=["timestamp_start"]
    ).dropna(subset=["throughput_per_hour", "downtime_ratio"])

    downtime_df = pd.read_csv(
        os.path.join(featured_dir, "downtime_features.csv"),
        parse_dates=["downtime_start_ts"]
    )

    downtime_dur_summary = pd.read_csv(
        os.path.join(tables_dir, "downtime_duration_summary.csv")
    )

    reconciliation_df = pd.read_csv(
        os.path.join(featured_dir, "event_hour_reconciliation.csv"),
        parse_dates=["timestamp_start"]
    ).dropna(subset=["event_vs_hour_downtime_diff_sec"]).sort_values("timestamp_start")

    # -----------------------------
    # Downtime density pivot (weekday x hour)
    # -----------------------------
    downtime_df["hour"] = downtime_df["downtime_start_ts"].dt.hour
    downtime_df["weekday"] = downtime_df["downtime_start_ts"].dt.dayofweek

    pivot = (
        downtime_df.groupby(["weekday", "hour"])["downtime_duration_sec"]
        .sum()
        .unstack(fill_value=0)
    )
    day_labels = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"][: len(pivot)]

    # -----------------------------
    # Build Figure
    # -----------------------------
    fig = go.Figure()

    # 0 — Daily Efficiency Trend
    fig.add_trace(go.Scatter(
        x=daily_df["date"].astype(str),
        y=daily_df["efficiency"],
        mode="lines+markers",
        name="Daily Efficiency Trend",
        line=dict(color=MAIN_COLOR, width=3),
        visible=True
    ))

    # 1 — Hourly Efficiency by Hour-of-Day
    fig.add_trace(go.Bar(
        x=hourly_density_df["hour"].astype(str),
        y=hourly_density_df["mean_efficiency"],
        name="Mean Hourly Efficiency",
        marker_color=MAIN_COLOR,
        visible=False
    ))

    # 2 — Throughput vs Downtime Ratio scatter
    fig.add_trace(go.Scatter(
        x=hourly_df["downtime_ratio"],
        y=hourly_df["throughput_per_hour"],
        mode="markers",
        name="Throughput vs Downtime",
        marker=dict(color=MAIN_COLOR, opacity=0.7, size=6),
        visible=False
    ))

    # 3 — Pause Ratio Trend
    fig.add_trace(go.Scatter(
        x=daily_df["date"].astype(str),
        y=daily_df["pause_ratio"],
        mode="lines+markers",
        name="Daily Pause Ratio",
        line=dict(color=MAIN_COLOR, width=3),
        visible=False
    ))

    # 4 — Downtime Duration Summary bar
    fig.add_trace(go.Bar(
        x=downtime_dur_summary["metric"],
        y=downtime_dur_summary["value"],
        name="Downtime Duration Statistics",
        marker_color=MAIN_COLOR,
        visible=False
    ))

    # 5 — Downtime Density Heatmap (dedicated axes)
    fig.add_trace(go.Heatmap(
        z=pivot.values,
        x=pivot.columns.astype(str),
        y=day_labels,
        colorscale=HEATMAP_COLORSCALE,
        colorbar=dict(title="Total Downtime (sec)"),
        name="Downtime Density Heatmap",
        visible=False,
        xaxis="x2",
        yaxis="y2"
    ))

    # 6 — Consistency Validation scatter
    fig.add_trace(go.Scatter(
        x=list(range(len(reconciliation_df))),
        y=reconciliation_df["event_vs_hour_downtime_diff_sec"],
        mode="markers",
        name="Downtime Reconciliation Gap",
        marker=dict(color=MAIN_COLOR, opacity=0.7, size=5),
        visible=False
    ))

    # -----------------------------
    # Base Layout
    # -----------------------------
    fig.update_layout(
        title=dict(text="Daily Efficiency Trend", x=0.5),
        plot_bgcolor="white",
        paper_bgcolor="white",
        font=dict(color=DARK_COLOR),
        margin=dict(t=80),
        yaxis=dict(
            title="Value",
            gridcolor=GRID_COLOR,
            showgrid=True,
            zeroline=False,
            showline=True,
            linecolor=BORDER_COLOR,
            mirror=True,
            showticklabels=True
        ),
        xaxis=dict(
            showgrid=False,
            showline=True,
            linecolor=BORDER_COLOR,
            mirror=True
        ),
        bargap=0.4,
    )

    # Heatmap dedicated axes
    fig.update_layout(
        xaxis2=dict(
            title="Hour of Day",
            type="category",
            overlaying="x",
            side="bottom",
            visible=False
        ),
        yaxis2=dict(
            title="Weekday",
            type="category",
            overlaying="y",
            side="left",
            visible=False,
            showticklabels=True
        )
    )

    plot_html = pio.to_html(
        fig,
        full_html=False,
        include_plotlyjs="cdn",
        div_id="mfgDashboard"
    )

    # -----------------------------
    # Write HTML
    # -----------------------------
    with open(output_html_path, "w", encoding="utf-8") as f:
        f.write(f"""
<html>
<head>
<title>Manufacturing Downtime Analytics Dashboard</title>
</head>

<body style="background:#FAFEFE; font-family:Arial;">

<h1 style="color:{DARK_COLOR}; text-align:center;">
Manufacturing Downtime Analytics Dashboard
</h1>

<div style="text-align:center; margin-top:20px;">
<select id="metricSelect" style="
    padding:10px 16px;
    border-radius:10px;
    border:1px solid #BFDCDC;
    font-size:15px;
    color:{DARK_COLOR};
" onchange="updateChart()">
    <option value="0">Daily Efficiency Trend</option>
    <option value="1">Hourly Efficiency by Hour-of-Day</option>
    <option value="2">Throughput vs Downtime Ratio</option>
    <option value="3">Daily Pause Ratio Trend</option>
    <option value="4">Downtime Duration Statistics</option>
    <option value="5">Downtime Density Heatmap</option>
    <option value="6">Event-Level Downtime Reconciliation</option>
</select>
</div>

<div style="
    max-width: 1150px;
    margin: 25px auto;
    padding: 30px;
    border: 1px solid #E6F2F2;
    border-radius: 18px;
    box-shadow: 0 10px 25px rgba(0,0,0,0.08);
    background-color: white;
">
{plot_html}
</div>

<script>
function updateChart() {{
    const val = parseInt(document.getElementById("metricSelect").value);
    let visibility = [false, false, false, false, false, false, false];
    visibility[val] = true;

    let titles = [
        "Daily Efficiency Trend",
        "Mean Hourly Efficiency by Hour-of-Day",
        "Throughput vs Downtime Ratio (per Hour)",
        "Daily Pause Ratio Trend",
        "Downtime Duration Statistics",
        "Downtime Density Heatmap (Weekday × Hour)",
        "Event-Level vs Hourly Downtime Reconciliation Gap"
    ];

    Plotly.restyle("mfgDashboard", "visible", visibility);
    Plotly.relayout("mfgDashboard", {{
        title: {{ text: titles[val], x: 0.5 }}
    }});

    if (val == 5) {{
        Plotly.relayout("mfgDashboard", {{
            "xaxis2.visible": true,
            "yaxis2.visible": true,
            "yaxis.showticklabels": false
        }});
    }} else {{
        Plotly.relayout("mfgDashboard", {{
            "xaxis2.visible": false,
            "yaxis2.visible": false,
            "yaxis.showticklabels": true
        }});
    }}
}}
</script>

</body>
</html>
""")

    print("Dashboard created at:", output_html_path)
