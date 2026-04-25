from __future__ import annotations

import json
import math
import os
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from eu_energy_intelligence.ingestion.entsoe_client import ProductionEntsoeClient

try:
    import pandas as pd
except ImportError:  # pragma: no cover - optional dependency
    pd = None  # type: ignore[assignment]

try:
    from entsoe import EntsoePandasClient
except ImportError:  # pragma: no cover - optional dependency
    EntsoePandasClient = None  # type: ignore[assignment]

try:
    from prophet import Prophet
except ImportError:  # pragma: no cover - optional dependency
    Prophet = None  # type: ignore[assignment]

try:
    import mlflow
except ImportError:  # pragma: no cover - optional dependency
    mlflow = None  # type: ignore[assignment]


def _utc_now() -> str:
    return datetime.now(UTC).isoformat()


def _require_ml_stack() -> None:
    missing: list[str] = []
    if pd is None:
        missing.append("pandas")
    if EntsoePandasClient is None:
        missing.append("entsoe-py")
    if Prophet is None:
        missing.append("prophet")
    if missing:
        missing_text = ", ".join(missing)
        raise ImportError(
            f"Backtesting requires {missing_text}. "
            "Use the Python 3.11 environment with the ML extras installed."
        )


@dataclass(slots=True)
class BacktestMetrics:
    mae: float
    rmse: float
    mape_pct: float
    smape_pct: float
    actual_count: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "mae": self.mae,
            "rmse": self.rmse,
            "mape_pct": self.mape_pct,
            "smape_pct": self.smape_pct,
            "actual_count": self.actual_count,
        }


def _mae(actual: list[float], predicted: list[float]) -> float:
    return sum(abs(a - p) for a, p in zip(actual, predicted, strict=False)) / len(actual)


def _rmse(actual: list[float], predicted: list[float]) -> float:
    return math.sqrt(
        sum((a - p) ** 2 for a, p in zip(actual, predicted, strict=False)) / len(actual)
    )


def _mape_pct(actual: list[float], predicted: list[float]) -> float:
    non_zero = [(a, p) for a, p in zip(actual, predicted, strict=False) if a != 0]
    if not non_zero:
        return 0.0
    return 100.0 * sum(abs((a - p) / a) for a, p in non_zero) / len(non_zero)


def _smape_pct(actual: list[float], predicted: list[float]) -> float:
    pairs = [(a, p) for a, p in zip(actual, predicted, strict=False) if (abs(a) + abs(p)) > 0]
    if not pairs:
        return 0.0
    return 100.0 * sum((2 * abs(a - p) / (abs(a) + abs(p))) for a, p in pairs) / len(pairs)


def compute_backtest_metrics(actual: list[float], predicted: list[float]) -> BacktestMetrics:
    if not actual or not predicted:
        raise ValueError("Backtest metrics require non-empty actual and predicted lists")
    if len(actual) != len(predicted):
        raise ValueError("actual and predicted lists must have the same length")
    return BacktestMetrics(
        mae=round(_mae(actual, predicted), 6),
        rmse=round(_rmse(actual, predicted), 6),
        mape_pct=round(_mape_pct(actual, predicted), 6),
        smape_pct=round(_smape_pct(actual, predicted), 6),
        actual_count=len(actual),
    )


def fetch_day_ahead_price_series(
    zone: str,
    start_date: str,
    end_date: str,
    api_key: str | None = None,
):
    _require_ml_stack()
    key = api_key or os.getenv("ENTSOE_API_KEY")
    if not key:
        raise ValueError("ENTSOE_API_KEY not set in environment")
    client = EntsoePandasClient(api_key=key)
    start, end = ProductionEntsoeClient.pd_timestamps(
        datetime.fromisoformat(start_date).date(),
        datetime.fromisoformat(end_date).date(),
    )
    return client.query_day_ahead_prices(ProductionEntsoeClient.eic(zone), start=start, end=end)


def build_daily_price_frame(
    zone: str,
    start_date: str,
    end_date: str,
    api_key: str | None = None,
):
    _require_ml_stack()
    series = fetch_day_ahead_price_series(zone, start_date, end_date, api_key=api_key)
    if isinstance(series, pd.DataFrame):
        if series.shape[1] != 1:
            raise ValueError("Expected a single-column price frame from ENTSO-E")
        series = series.iloc[:, 0]
    daily = series.resample("D").mean().reset_index()
    daily.columns = ["ds", "y"]
    daily["zone"] = zone
    daily["ds"] = pd.to_datetime(daily["ds"]).dt.tz_localize(None)
    start_ts = pd.Timestamp(start_date)
    end_ts = pd.Timestamp(end_date)
    daily = daily[(daily["ds"] >= start_ts) & (daily["ds"] <= end_ts)]
    daily = daily.dropna(subset=["y"]).reset_index(drop=True)
    return daily


def _json_dump(path: Path, payload: Any) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    return str(path)


def run_prophet_price_backtest(
    zone: str = "NL",
    train_start: str = "2024-01-01",
    train_end: str = "2024-12-31",
    test_start: str = "2025-10-01",
    test_end: str = "2025-10-31",
    processed_base_dir: str = "data/processed",
    api_key: str | None = None,
) -> dict[str, Any]:
    _require_ml_stack()

    train_df = build_daily_price_frame(zone, train_start, train_end, api_key=api_key)
    test_df = build_daily_price_frame(zone, test_start, test_end, api_key=api_key)
    if len(train_df) < 60:
        raise ValueError(f"Not enough training rows for Prophet: {len(train_df)}")
    if test_df.empty:
        raise ValueError("No actual test prices returned for the requested evaluation period")

    model = Prophet(yearly_seasonality=True, weekly_seasonality=True, daily_seasonality=False)
    model.fit(train_df[["ds", "y"]])

    future = test_df[["ds"]].copy()
    forecast = model.predict(future)[["ds", "yhat", "yhat_lower", "yhat_upper"]]
    joined = test_df.merge(forecast, on="ds", how="inner")
    metrics = compute_backtest_metrics(
        actual=joined["y"].astype(float).tolist(),
        predicted=joined["yhat"].astype(float).tolist(),
    )

    prediction_rows = [
        {
            "zone": zone,
            "date": row.ds.date().isoformat(),
            "actual_price_eur_mwh": round(float(row.y), 4),
            "predicted_price_eur_mwh": round(float(row.yhat), 4),
            "predicted_lower": round(float(row.yhat_lower), 4),
            "predicted_upper": round(float(row.yhat_upper), 4),
            "absolute_error": round(abs(float(row.y) - float(row.yhat)), 4),
        }
        for row in joined.itertuples()
    ]

    mlflow_run_id: str | None = None
    if mlflow is not None:
        mlflow.set_experiment("/experiments/emit_price_backtests")
        with mlflow.start_run(run_name=f"prophet_backtest_{zone}_{test_start}_{test_end}") as run:
            mlflow.log_params(
                {
                    "zone": zone,
                    "train_start": train_start,
                    "train_end": train_end,
                    "test_start": test_start,
                    "test_end": test_end,
                    "model_type": "prophet_daily_backtest",
                }
            )
            mlflow.log_metrics(metrics.to_dict())
            mlflow_run_id = run.info.run_id

    output_dir = Path(processed_base_dir) / "ml" / "backtests"
    prediction_path = _json_dump(
        output_dir / f"{zone.lower()}_{train_start}_{train_end}_to_{test_start}_{test_end}_predictions.json",
        prediction_rows,
    )
    summary = {
        "zone": zone,
        "train_start": train_start,
        "train_end": train_end,
        "test_start": test_start,
        "test_end": test_end,
        "train_row_count": int(len(train_df)),
        "test_row_count": int(len(test_df)),
        "metrics": metrics.to_dict(),
        "prediction_path": prediction_path,
        "mlflow_run_id": mlflow_run_id,
        "generated_at_utc": _utc_now(),
    }
    summary_path = _json_dump(
        output_dir / f"{zone.lower()}_{train_start}_{train_end}_to_{test_start}_{test_end}_summary.json",
        summary,
    )
    summary["summary_path"] = summary_path
    return summary
