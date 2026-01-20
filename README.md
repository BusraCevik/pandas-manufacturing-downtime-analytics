# 🏭 pandas-manufacturing-downtime-analytics

A **Pandas-based manufacturing operations analytics** project focused on analyzing production throughput, downtime behavior, and operational inefficiencies in an industrial beverage bottling line.

The project follows a **layered and modular data pipeline architecture**, transforming raw event logs and time-series operational data into validated analytical features, operational KPIs, and interpretable performance insights.

Unlike traditional business analytics projects, this project operates on **event-level production logs and multi-resolution time-series data**, aiming to understand *where, when, and how production time is lost inside a real manufacturing process*.

This project answers questions such as:

- Where does downtime concentrate across the production timeline?
- Which hours and operating windows suffer the highest inefficiencies?
- How do micro-level downtime events propagate into hourly and daily performance losses?
- How consistent are operational metrics across multiple aggregation layers?

---

## 📌 Project Overview

Operational efficiency in manufacturing cannot be understood only through daily summaries or total production numbers.  
True optimization requires analyzing how machines behave at the event level and validating how those behaviors aggregate into hourly and daily operational performance.

This project focuses on:

- Event-level downtime intelligence  
- Hourly throughput and efficiency behavior  
- Daily operational stability and performance validation  
- Cross-table consistency checks between raw logs and aggregated datasets  

The project provides:

- A multi-stage data pipeline (`raw → cleaned → featured → analytics`)
- Event sessionization and downtime duration engineering
- Multi-resolution time-series validation (event → hour → day)
- Throughput vs downtime correlation analysis
- Operational efficiency and loss pattern detection
- Static visualizations and optional dashboards

---

## 📊 Dataset

[**Industrial Production - Beverage Bottling Line**](https://www.kaggle.com/datasets/gabrielaugustodavid/industrial-production-beverage-bottling-line)  


The dataset contains four complementary tables representing different aggregation layers of the same production system.

### Tables

- **downtime_event_log.csv**  
  Event-level downtime logs including start time, end time, and recorded downtime duration.

- **hourly_operation_breakdown.csv**  
  Hourly operational metrics including monitored time, operation time, downtime, and efficiency.

- **processed_hourly.csv**  
  Hourly production throughput measured in gallons.

- **daily_operation_summary.csv**  
  Daily production summaries including efficiency, operation time, pause time, and production volume.

**Data organization:**
- Raw data: `data/raw`
- Cleaned data: `data/cleaned`
- Feature-engineered data: `data/featured`

---

## 📈 Example Outputs

### ⏱️ Downtime Intelligence
- Downtime event duration distribution  
- Downtime clustering and burst detection  
- Hourly downtime density heatmap  
- Validation of recorded downtime vs computed durations  
- Recovery time analysis  

![](outputs/figures/downtime_event_distribution.png)  
![](outputs/figures/downtime_density_heatmap.png)

---

### ⚙️ Hourly Performance Analysis
- Production throughput vs downtime correlation  
- Zero-production window detection  
- Hourly efficiency decay patterns  
- Throughput volatility analysis  
- Cross-table validation against hourly operational summaries  

![](outputs/figures/hourly_efficiency_trend.png)  
![](outputs/figures/throughput_vs_downtime.png)

---

### 📆 Daily Operational Stability
- Daily efficiency trend analysis  
- Pause ratio distribution  
- Best vs worst operational days  
- Stability and variance analysis  
- Product-level performance comparison  

![](outputs/figures/daily_efficiency_trend.png)  
![](outputs/figures/pause_ratio_distribution.png)

---

### 🔎 Cross-Level Consistency Validation
- Event-level vs hourly downtime reconciliation  
- Hourly vs daily aggregation consistency  
- Outlier detection and anomaly flagging  
- Data quality validation reports  

![](outputs/figures/consistency_validation.png)

---

### 🌐 Interactive Dashboard

- Interactive Dashboard Demo  
![](docs/demo.gif)

🖱️ **Live Dashboard:**  
<a href="https://busracevik.github.io/pandas-manufacturing-downtime-analytics/index.html" target="_blank">View Interactive Dashboard</a>

---


## 🔍 Key Insight Example

Operational losses are not evenly distributed across the production timeline.  
A small number of clustered downtime windows account for a disproportionate share of total lost production time, confirming a Pareto-like behavior in operational inefficiencies.

Cross-validation between event logs and hourly summaries reveals minor aggregation gaps, highlighting the importance of validation layers in real-world industrial data pipelines.

---

## 📁 Project Structure

```text
pandas-manufacturing-downtime-analytics/
│
├── data/
│   ├── raw/
│   ├── cleaned/
│   └── featured/
│
├── outputs/
│   ├── tables/
│   └── figures/
│
├── docs/
│   ├── demo.gif
│   └── index.html
│
├── src/
│   ├── data_processing/
│   │   ├── data_preparation.py
│   │   └── feature_engineering.py
│   │
│   ├── analysis/
│   │   ├── event_analysis.py
│   │   ├── hourly_analysis.py
│   │   └── daily_analysis.py
│   │
│   └── visualization/
│       ├── plots.py
│       └── dashboard.py
│
├── main.py                 # Entry point
├── requirements.txt
└── README.md

```
## 🛠 Technologies Used

- **Python** – Core programming language  
- **Pandas** – Data preprocessing and analytics  
- **NumPy** – Numerical computations  
- **Matplotlib** – Static visualizations  
- **Plotly** – Interactive dashboards  

---

## 🧠 Analytical Approach

This project emphasizes **operational interpretability and process understanding** rather than predictive modeling.

No machine learning models are used.

Instead, the analysis relies on:

- Event-level time reconstruction  
- Time-based aggregation and validation  
- Throughput and downtime correlation analysis  
- Multi-resolution consistency checks  

The focus is on explaining **where operational losses occur and how they propagate through the production timeline**.

---

## 📐 Core Metrics & Definitions

### ⏱️ Downtime Duration

**Definition:**

$$
\text{Downtime Duration} = \text{Downtime End Time} - \text{Downtime Start Time}
$$

**Explanation:**  
Represents the length of each downtime event.

---

### ⏳ Hourly Downtime Ratio

**Definition:**

$$
\text{Downtime Ratio}_{hour} = \frac{\text{Downtime}_h}{\text{Monitored Time}_h}
$$

**Explanation:**  
Measures the proportion of lost production time per hour.

---

### ⚙️ Throughput

**Definition:**

$$
\text{Throughput} = \frac{\text{Total Production Volume}}{\text{Operation Time}}
$$

**Explanation:**  
Measures production efficiency over time.

---

### 📊 Daily Efficiency

**Definition:**

$$
\text{Efficiency}_{day} = \frac{\text{Operation Time}}{\text{Monitored Time}}
$$

**Explanation:**  
Represents overall daily operational utilization.

