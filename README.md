# eda-ai-agent

A generic, config-driven EDA (Exploratory Data Analysis) framework for tabular data.
Point it at any folder of CSV/Parquet/Excel files, write a YAML config, and get a
complete Markdown report with null coverage, distributions, anomaly alerts, temporal
trends, concentration analysis, and optional HTML profiles — all from one command.

---

## Quick start

```bash
# 1. Install dependencies (requires uv)
uv sync

# 2. Place your data files in inputs/
# 3. Write (or copy) a config
cp configs/example.yaml configs/my_project.yaml
# edit configs/my_project.yaml …

# 4. Run
uv run python scripts/run_eda.py --config configs/my_project.yaml

# 5. Open the report
open outputs/relatorio_eda.md
```

Optional flags:

| Flag | Effect |
|---|---|
| `--output path/report.md` | Override output report path |
| `--no-plots` | Skip PNG chart generation |
| `--no-profile` | Skip ydata-profiling HTML reports |

---

## Repository layout

```
eda-ai-agent/
├── scripts/
│   ├── run_eda.py              # CLI runner (entry point)
│   ├── schema_inspector.py     # Lightweight schema summary tool
│   └── eda/                    # Reusable Python package
│       ├── __init__.py
│       ├── loader.py           # File loading (CSV, Parquet, Excel, glob)
│       ├── profiler.py         # Null coverage, stats, anomalies, plots
│       ├── temporal.py         # Time distributions, duration, trends
│       ├── concentration.py    # Group aggregation, risk score, correlations
│       └── report.py           # Fluent Markdown report builder
├── configs/
│   └── example.yaml            # Annotated config template
├── inputs/                     # Drop your data files here (gitignored)
├── outputs/                    # Generated reports and plots (gitignored)
├── tests/                      # pytest test suite
├── pyproject.toml
└── README.md
```

---

## YAML config reference

```yaml
name: "Report title"
output_dir: "outputs"

datasets:
  my_dataset:
    path: "inputs/data*.csv"        # glob pattern — first match loaded
    sample_rows: 500000             # optional row limit
    usecols: [col_a, col_b]         # optional column selection
    target_col: status              # highlighted in profile output
    primary_date_col: created_at    # drives temporal distribution section
    group_col: region               # used in target crosstab
    duration:                       # optional: compute a duration column
      start_col: open_date
      end_col: close_date
      output_col: duration_h
      unit: h                       # s | m | h | d
    profiling:
      mode: minimal                 # minimal | explorative
      title: "My Dataset"

concentration:
  group_col: region                 # shared key across all aggregated tables

  groups:
    source_a:
      dataset: my_dataset
      group_col: region
      metrics:
        n_records: {col: id,     func: nunique}
        avg_value: {col: value,  func: mean}
        pct_flag:  {col: flag,   func: pct_true}

    source_b:
      dataset: events
      group_col: user_id
      via:                          # translate user_id → region via bridge join
        - dataset: my_dataset
          left_on: user_id
          right_on: id
          carry: region
      metrics:
        n_events: {col: event_type, func: count}

  risk_score:
    metrics:
      - col: avg_value
        weight: 1.0
        higher_is_worse: false
      - col: n_events
        weight: 2.0
        higher_is_worse: true
```

### Available aggregation functions

| Name | Description |
|---|---|
| `count` | Non-null row count |
| `nunique` | Distinct value count |
| `sum` | Sum |
| `mean` | Mean |
| `median` | Median |
| `max` / `min` | Extremes |
| `std` | Standard deviation |
| `p10` / `p90` / `p95` | Percentiles |
| `notnull_pct` | % of non-null values |
| `pct_true` | % of truthy values |
| `gt{N}_pct` | % of values > N (e.g. `gt24_pct`) |

### `via` — multi-hop joins

The `via` key under a concentration group lets you translate a dataset's native
group key into the panel's common key before aggregation, using one or more bridge
datasets that are already loaded.

```yaml
via:
  - dataset: bridge_table   # must be defined under `datasets`
    left_on: local_key      # column in the current dataset
    right_on: bridge_key    # column in the bridge dataset
    carry: target_key       # column to carry over → becomes the new group key
```

Multiple hops are chained in sequence: the `carry` column of hop N becomes the
`left_on` of hop N+1.

---

## Module API

### `eda.loader`

```python
from eda.loader import load_dataset, load_all, find_files, load_file

df = load_dataset("inputs/data*.csv", sample_rows=100_000, usecols=["id", "value"])
all_df = load_all("inputs/part_*.parquet")
```

### `eda.profiler`

```python
from eda.profiler import (
    null_coverage, describe_numeric, value_counts_pct, bin_distribution,
    detect_anomalies, plot_dataframe, profile_dataframe, crosstab_rate,
)

profile = profile_dataframe(df, name="orders", target_col="churn", verbose=False)
# profile keys: name, shape, null_coverage, numeric, categorical, anomalies

saved = plot_dataframe(df, name="orders", output_dir="outputs/plots")
# returns List[Path] of saved PNG files

rate_table = crosstab_rate(df, group_col="region", target_col="churn")
```

**Anomaly detection rules:**

| Tag | Condition |
|---|---|
| `[CONSTANTE]` | Column has ≤ 1 unique value |
| `[NULOS CRÍTICOS]` | > 80 % null |
| `[NULOS ALTOS]` | > 30 % null |
| `[NEGATIVO]` | Numeric column contains negative values |
| `[OUTLIER EXTREMO]` | max > p99 × 10 |
| `[ALTA CARDINALIDADE]` | Object column with > 90 % unique values and > 100 distinct values |

### `eda.temporal`

```python
from eda.temporal import (
    parse_dates, period_distribution, hourly_distribution,
    weekday_distribution, monthly_heatmap, compute_duration,
    trend_by_period, category_trend,
)

dist = period_distribution(df["created_at"], freq="M")   # M W D Q Y
dur  = compute_duration(df["start"], df["end"], unit="h")
```

### `eda.concentration`

```python
from eda.concentration import (
    aggregate_by_key, merge_tables, compute_risk_score,
    spearman_corr, double_high, top_n, make_gt_pct, make_pct_value,
)

agg = aggregate_by_key(df, "region", {
    "n_customers": ("id",    "nunique"),
    "avg_value":   ("value", "mean"),
    "pct_high":    ("value", make_gt_pct(1000)),
}, min_group_size=10)

panel = merge_tables([agg_a, agg_b], on="region")
panel["score"] = compute_risk_score(panel, ["n_customers", "avg_value"])
```

### `eda.report`

```python
from eda.report import MarkdownReport

report = MarkdownReport("My Report", description="Config: config.yaml")
report.h2("Section").metric("Rows", f"{len(df):,}").table(df.head())
report.alerts(["Warning A", "Warning B"], level="warning")
report.save("outputs/report.md")
```

Alert levels: `info` | `warning` | `error` | `success`

---

## Schema inspector (quick scan)

For a fast schema-only scan without full EDA:

```bash
uv run python scripts/schema_inspector.py \
  --data-dir inputs \
  --output schema_summary.md \
  --json-output schema_summary.json \
  --sample-rows 5000
```

---

## Running tests

```bash
uv run pytest tests/ -v
uv run pytest tests/ --cov=scripts/eda --cov-report=term-missing
```

---

## Outputs

After running `run_eda.py`, the `outputs/` directory contains:

```
outputs/
├── relatorio_eda.md        # Main Markdown report
├── plots/
│   ├── <dataset>__<col>__hist_box.png   # Histogram + boxplot (numeric)
│   └── <dataset>__<col>__barplot.png    # Bar chart (categorical)
└── profiles/
    └── <dataset>_profile.html           # ydata-profiling HTML report
```
