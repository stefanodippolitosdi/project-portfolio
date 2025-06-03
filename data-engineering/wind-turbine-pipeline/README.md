---
output:
  pdf_document: default
  html_document: default
---
# Wind Turbine Data‑Processing Pipeline  
Senior Data Engineer — Technical Task

---

## Overview
This repository contains a **self‑contained Python module** (`wind_pipeline.py`) that builds a
scalable, testable data‑processing pipeline for a wind‑turbine farm.  
The pipeline ingests raw CSV feeds, cleans and validates the data, computes
per‑turbine **daily statistics**, flags anomalous readings (±2 σ), and persists the
results for downstream analytics.

---

## Features
| Stage | Function | Output |
|-------|----------|--------|
| **Load** | Read & concatenate multiple daily CSVs | Unified `DataFrame` |
| **Clean** | ✓ Deduplicate<br>✓ Timestamp parsing (UTC)<br>✓ Median imputation for missing power<br>✓ Percentile‑based outlier removal | `cleaned_data.csv` |
| **Stats** | Min / Max / Mean per turbine & calendar day | `summary_statistics.csv` |
| **Anomaly** | Flags readings ±2 standard deviations from same‑day mean | `anomalies.csv` |
| **Persist** | Saves artefacts to `./output/` | 3 ready‑to‑load CSVs |

The code is fully type‑hinted, PEP 257‑documented, and vectorised for
performance on large datasets.

---

## Quick Start

```bash
# 1 Install deps (create venv first if desired)
pip install -r requirements.txt

# 2 Place raw CSVs in project root (here 3 sample CSVs file are present) or adjust paths
python wind_pipeline.py
```

All results are written to **`./output/`**.

---

## Requirements
* Python ≥ 3.9  
* `pandas >= 2.2`  
* `numpy >= 1.26`

(See `requirements.txt`.)

---

## File Structure
```
wind-turbine-pipeline/
├── wind_pipeline.py          # Main pipeline (end‑to‑end)
├── README.md                 # ← you are here
├── requirements.txt
├── data_group_1.csv          # Sample raw data (group of 5 turbines)
├── data_group_2.csv
├── data_group_3.csv
└── output/                   # Auto‑generated artefacts
    ├── cleaned_data.csv
    ├── summary_statistics.csv
    └── anomalies.csv
```

---

## Extensibility
* **Database** — Replace `save_outputs()` with a SQLAlchemy loader.  
* **Distributed Scale** — Port cleaning & stats functions to **PySpark** / **Dask**; the
  logic remains unchanged.  
* **Scheduling** — Run `wind_pipeline.py` daily via cron, Airflow, or similar.

---

## Assumptions
1. Each CSV always contains `timestamp`, `turbine_id`, `power_output` (MW).  
2. Power output is non‑negative; extreme negative/high values are treated as outliers.  
3. Daily σ = 0 ⇒ no anomalies (constant output).  
4. Median imputation is acceptable for occasional sensor gaps.

---

## License
© 2025 RLDatix (task context). This code is provided solely for the interview
process and is not licensed for production use without permission.
