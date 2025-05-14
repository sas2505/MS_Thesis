# Bench-Tool

**Bench-Tool** is a Python-based command-line interface (CLI) developed to support benchmarking experiments for quality-aware stream processing in **Data Stream Management Systems (DSMS)**. It is specifically designed to work alongside the Odysseus DSMS and facilitates dataset preparation, data quality simulation, query validation, and performance analysis.

## 📌 Features

- ✅ Dataset preprocessing (splitting, extraction, quality simulation)
- ✅ Controlled injection of data quality issues:
  - Inaccuracies (outliers)
  - Missing values
  - Data delays (timeliness)
- ✅ Manual computation and verification of data quality metrics
- ✅ Performance measurement (latency and throughput)
- ✅ Visual comparison across multiple experimental runs


---

## 🧰 Commands Overview

### `preprocess`
Handles dataset splitting, extraction, and injection of data quality issues.

- `split` – Divides multi-sensor dataset into individual sensor files.
- `extract` – Selects specific time window (e.g., 30 days).
- `prepare` – Injects inaccuracies, missing values, and timeliness delays.

### `data-quality`
Used for data quality metric calculation and validation.

- `show` – Manually compute accuracy, completeness, and timeliness.
- `verify` – Compare Odysseus query output with manually computed results.

### `benchmark`
Performs performance evaluation of the DSMS.

- `analyze` – Measures latency and throughput for streaming windows.
- `compare` – Visualizes performance differences across runs.

---

## ⚙️ Configuration

All quality simulation parameters are set via a `config.yaml` file:

```yaml
DEVIATION: 0.05              # Standard deviation for noise
OUTLIER_FACTOR: 2            # Magnitude of outliers
OUTLIER_PERCENTAGE: 0.05     # Percentage of outliers
MISSING_PERCENTAGE: 0.1      # Percentage of missing values
VOLATILITY: 2000             # Time window for valid data (ms)
OUTDATED_PERCENTAGE: 0.1     # Percentage of outdated data
CHUNK_SIZE: 35000            # Rows per chunk for processing
```

<!-- ## 🧪 Example Usage

### Split dataset
```bench_tool preprocess split --input data.csv```

### Prepare dataset with quality issues
```bench_tool preprocess prepare --config config.yaml```

### Verify quality results
```bench_tool data-quality verify --input results.csv```

### Analyze performance
```bench_tool benchmark analyze --input output.csv``` -->

## Installation:

To install the `bench_tool-1.0.0-py3-none-any.whl` package, you can use the following command:
 
```bash
pip install bench_tool-1.0.0-py3-none-any.whl
```

Ensure that you have Python 3 and `pip` installed on your system. If the `.whl` file is not in the current directory, provide the full path to the file.

Example:
```bash
pip install /path/to/bench_tool-1.0.0-py3-none-any.whl
```
