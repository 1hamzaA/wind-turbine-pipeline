# Wind Turbine Data Pipeline

A PySpark pipeline for processing wind turbine power generation data.

## Project Structure

```text
wind-turbine-pipeline/
├── data/
│   └── raw/
│       ├── data_group_1.csv
│       ├── data_group_2.csv
│       └── data_group_3.csv
├── src/
│   └── wind_turbine_pipeline/
│       ├── __init__.py
│       └── ingestion.py
├── tests/
│   ├── conftest.py
│   └── test_ingestion.py
├── pyproject.toml
├── poetry.lock
├── .gitignore
└── README.md
```

## Requirements

- Python 3.11
- Java 17
- Poetry

PySpark requires Java to run.

## Setup

Clone the repository and install the dependencies:

```powershell
git clone <repository-url>
cd wind-turbine-pipeline
python -m poetry install
```

## Running Tests

Run the test suite with:

```powershell
python -m poetry run pytest
```

## Current Implementation

### Ingestion

The ingestion module reads the raw CSV files into a PySpark DataFrame using an explicit schema.

The input data contains:

- `timestamp`
- `turbine_id`
- `wind_speed`
- `wind_direction`
- `power_output`

The ingestion functionality is covered by pytest tests using a small test dataset.

## Pipeline

The planned pipeline is:

```text
Raw CSV
   ↓
Ingestion
   ↓
Cleaning
   ↓
Analysis
   ↓
Output
```

The remaining stages will be added incrementally.