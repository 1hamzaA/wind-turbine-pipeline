# Wind Turbine Data Pipeline

A proof-of-concept PySpark pipeline for processing and analysing wind turbine power generation data.

The pipeline ingests multiple CSV files, cleans the data, calculates time-windowed statistics, identifies anomalies, and writes the processed results to CSV.

## Project Structure

```text
wind-turbine-pipeline/
├── data/
│   ├── raw/
│   │   ├── data_group_1.csv
│   │   ├── data_group_2.csv
│   │   └── data_group_3.csv
│   └── processed/
│       ├── cleaned_turbine_data.csv
│       ├── summary_statistics.csv
│       └── anomalies.csv
├── src/
│   └── wind_turbine_pipeline/
│       ├── ingestion.py
│       ├── cleaning.py
│       ├── analysis.py
│       ├── storage.py
│       └── orchestrator.py
├── tests/
│   ├── conftest.py
│   ├── test_ingestion.py
│   ├── test_cleaning.py
│   └── test_analysis.py
├── run_pipeline.py
├── pyproject.toml
├── poetry.lock
├── .gitignore
└── README.md
```

## Requirements

- Python 3.11
- Java 17
- Poetry
- PySpark

## Setup

```powershell
git clone <repository-url>
cd wind-turbine-pipeline
python -m poetry install
```

## Dependencies

Runtime dependencies include PySpark and pandas.

Development dependencies are managed separately and include:

- pytest for testing
- pylint for code quality checks

Install all dependencies, including development dependencies, with:

```powershell
python -m poetry install
```

## Running the Pipeline

```powershell
python -m poetry run python run_pipeline.py
```

The pipeline currently processes:

```text
data/raw/data_group_1.csv
data/raw/data_group_2.csv
data/raw/data_group_3.csv
```

The default analysis period is 24 hours and can be changed using the `period_hours`
parameter.

## Running Tests

```powershell
python -m poetry run pytest
```

The tests cover ingestion, cleaning, summary statistics, and anomaly detection.

## Pipeline

```text
Raw CSV Files
      ↓
   Ingestion
      ↓
    Cleaning
      ↓
    Analysis
      ↓
    Storage
```

### Ingestion

Multiple CSV files are loaded into Spark using an explicit schema.

### Cleaning

Raw turbine data is validated and cleaned before analysis.

### Analysis

#### Summary Statistics

For each turbine and fixed time window, the pipeline calculates:

- Minimum power output
- Maximum power output
- Average power output

The window duration is configurable, with a default of 24 hours.

The output contains:

- `window_start`
- `window_end`
- `turbine_id`
- `min_power_output`
- `max_power_output`
- `avg_power_output`

#### Anomaly Detection

Power output is compared against the mean and standard deviation for the same
turbine and time window.

A measurement is considered anomalous when it falls outside:

```text
mean ± 2 × standard deviation
```

The result contains an `is_anomaly` flag.

## Output

The pipeline produces:

```text
data/processed/
├── cleaned_turbine_data.csv
├── summary_statistics.csv
└── anomalies.csv
```

## Storage Approach

CSV is used for this proof of concept because it is simple and easy to inspect.

The local Windows environment required additional Hadoop/`winutils.exe` configuration for Spark's native file writer. Given the small dataset and the limited scope of this technical exercise, Pandas is used to write the final DataFrames to CSV.
