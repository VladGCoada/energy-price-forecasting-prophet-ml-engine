# EU Energy Intelligence Platform

EU Energy Intelligence Platform is a modular data and intelligence project for working with European electricity market data from ENTSO-E.

At a simple level, it can:

- fetch power market data with an ENTSO-E API key
- clean and organize that data into Bronze, Silver, Gold, and Platinum layers
- validate contracts and data quality
- generate interpretable market-intelligence outputs
- run anomaly detection and forecasting
- backtest price forecasts against real historical prices

This repo is designed to be useful to both technical and non-technical users.

## What This Project Can Tell You

The platform can help answer questions like:

- What happened in a market during a given period?
- Which days were volatile, stressed, or unusual?
- How did prices, generation, load, and flows behave together?
- Are there signs of anomalies or regime shifts?
- Can a forecast trained on historical data predict a future period reasonably well?
- Is the data complete enough to trust the intelligence output?

Examples of outputs:

- daily renewable stability summaries
- price spike analysis
- market stress reports
- anomaly scores
- scenario simulations
- recommendations
- Prophet-based backtests

## Who Can Use This Repo

### Non-technical users

You can use the command-line examples in this README to:

- fetch ENTSO-E data
- run the local pipeline
- generate reports
- probe whether a zone and period have enough data
- run a forecast backtest

### Data analysts

You can use the processed JSON outputs in `data/processed/...` to:

- inspect daily market behavior
- compare actual vs predicted prices
- review anomaly outputs
- build notebooks and charts on top of the Gold and ML outputs

### Data engineers

You can use the codebase to:

- extend ingestion to more zones and corridors
- wire more datasets into the Silver/Gold/Platinum flow
- replace local JSON storage with more production-oriented storage
- integrate with Databricks, MLflow, or scheduled pipelines

### ML / forecasting users

You can use the project to:

- run local anomaly scoring
- run Prophet-based price backtests
- compare fallback forecasting vs Prophet
- extend the model with more regressors such as load, generation, and flows

## Architecture

The project follows a layered architecture:

- `Bronze`: raw extracted data
- `Silver`: validated and normalized timeseries
- `Gold`: interpretable analytical outputs
- `Platinum`: higher-order decision-support outputs
- `ML / Intelligence`: anomaly scoring, forecasting, backtesting, scenario analysis, and recommendations

Main package areas:

- `src/eu_energy_intelligence/ingestion`: ENTSO-E clients, XML parsing, overlap probes
- `src/eu_energy_intelligence/bronze`: raw-to-Bronze normalization
- `src/eu_energy_intelligence/silver`: Silver transformation logic
- `src/eu_energy_intelligence/gold`: Gold analytics and builders
- `src/eu_energy_intelligence/platinum`: advanced marts and higher-order outputs
- `src/eu_energy_intelligence/quality`: contracts and DQ validation
- `src/eu_energy_intelligence/intelligence`: anomaly scoring, forecasting, backtesting, reports, scenarios
- `src/eu_energy_intelligence/orchestration`: local, production, and 2030-style orchestration

## Technologies Used

The project is mainly built in **Python**, with a structure that works both as a local analytics project and as a foundation for a more production-style data platform.

Some of the main technologies used here are:

- **PySpark**
  - used as the platform-oriented processing layer and architectural target for scalable pipeline execution
- **pandas**
  - used for local time-series shaping, feature preparation, and forecasting workflows
- **ENTSOE-PY** and **requests**
  - used to access ENTSO-E market data and support direct API-driven ingestion
- **Pydantic** and **pydantic-settings**
  - used for typed configuration, settings management, and safer runtime behavior
- **YAML**
  - used for configuration and data contract definitions
- **scikit-learn**
  - used for anomaly detection, especially through `IsolationForest`
- **Prophet**
  - used for time-series forecasting and historical backtesting of market prices
- **MLflow**
  - used for experiment tracking in the forecasting workflow
- **pytest**
  - used for unit and integration testing across the platform

The repo also includes scaffolding for technologies that are common in more production-focused data platforms, such as **Databricks-style job packaging**, **lakehouse / medallion patterns**, and **infrastructure-oriented setup** for future expansion.

## Setup

### Recommended Python version

Use Python `3.11`.

Why:

- the richer ML stack is working reliably in the `3.11` environment
- the active `3.14` interpreter in this workspace did not have the ML dependencies installed
- `prophet` support is much more practical here under `3.11`

### Install

Create and activate a virtual environment, then install:

Core project:

```powershell
python -m venv .venv311
.\.venv311\Scripts\Activate.ps1
pip install -e .[dev]
```

Full ML stack:

```powershell
pip install -e .[dev,ml]
```

You can also install from:

```powershell
pip install -r requirements.txt
```

## ENTSO-E API Key

This project expects an ENTSO-E API key in the environment as:

```powershell
$env:ENTSOE_API_KEY="your_key_here"
```

Or in a local `.env` file:

```env
ENTSOE_API_KEY=your_key_here
```

The repo does not require you to commit secrets.

## Quick Start

### 1. Run a local live generation pipeline

Example for the Netherlands:

```powershell
python -m eu_energy_intelligence.orchestration.run_bronze `
  --live `
  --country-code NL `
  --period-start 202511010000 `
  --period-end 202511040000 `
  --processed-base-dir data/processed/nl_recent
```

This produces Bronze, Silver, and Gold outputs locally.

### 2. Inspect whether your inputs are complete enough for intelligence

```powershell
python -m eu_energy_intelligence.cli inspect-intelligence-inputs `
  --processed-base-dir data/processed/nl_recent
```

### 3. Probe whether ENTSO-E has overlapping datasets for a period

Example for Romania and Hungary:

```powershell
python -m eu_energy_intelligence.cli probe-entsoe-overlap `
  --zone RO `
  --flow-partner HU `
  --start-date 2025-11-01 `
  --end-date 2025-11-05
```

This checks whether prices, load, generation, and flows all exist for the same period.

### 4. Run local ML

```powershell
python -m eu_energy_intelligence.cli run-local-ml `
  --processed-base-dir data/processed/nl_recent `
  --zone NL `
  --horizon-intervals 8
```

Depending on the environment and available data, this will use:

- `IsolationForest` for anomaly scoring when `scikit-learn` is installed
- Prophet for price forecasting when `prophet` is installed and price history is present
- deterministic fallbacks when those are unavailable

### 5. Run a Prophet backtest

Example: train on 2024 and evaluate October 2025 for `NL`

```powershell
python -m eu_energy_intelligence.cli backtest-price-forecast `
  --zone NL `
  --train-start 2024-01-01 `
  --train-end 2024-12-31 `
  --test-start 2025-10-01 `
  --test-end 2025-10-31 `
  --processed-base-dir data/processed/backtest_nl_oct2025
```

This writes:

- prediction rows
- summary metrics such as `MAE`, `RMSE`, `MAPE`, `SMAPE`
- an MLflow run when MLflow is available

## Example User Paths

### If you are a market analyst

Use the repo to:

- fetch a zone and date range
- inspect Gold outputs
- run a backtest
- compare actual and predicted prices

Good commands:

- `run_bronze --live ...`
- `inspect-intelligence-inputs`
- `run-local-ml`
- `backtest-price-forecast`

### If you are a hiring manager or non-technical reviewer

This repo demonstrates:

- real external API integration
- layered data engineering design
- quality, contracts, and observability
- ML and forecasting workflows
- explainable intelligence outputs instead of only raw ETL

### If you are a developer

You can extend:

- zone coverage
- additional flow corridors
- richer Gold and Platinum outputs
- better regressors for Prophet
- new models beyond Prophet and `IsolationForest`

## Current ML Status

The repo now supports:

- local feature generation
- local anomaly scoring
- Prophet forecasting
- MLflow-backed forecast backtests

What this means practically:

- the ML architecture is implemented
- the ML workflow is runnable
- the quality of forecasts still depends on the training window, zone, and available regressors

In other words, the platform can now test forecasting honestly, not just expose forecast classes.

## Important Notes

- Some commands use local JSON outputs rather than a full production lakehouse setup.
- The local pipeline is strong for experimentation and prototyping.
- A forecast being runnable does not mean it is automatically good; the repo includes backtesting so forecast quality can be measured directly.
- Some intelligence outputs become weaker if only one dataset is present and others are missing.

## Running Tests

```powershell
pytest -q
```

## Main Config Files

- `pyproject.toml`
- `requirements.txt`
- `.env.example`
- `conf/base.yml`
- `conf/dev.yml`
- `conf/data_contracts/*.yaml`

## Documentation

- `docs/architecture.md`
- `docs/data_model.md`
- `docs/operations_runbook.md`
- `docs/data_dictionary.md`

## GitHub

Repository:

- `https://github.com/VladGCoada/EU-ENERGY-INTELLIGENCE-PLATFORM`
