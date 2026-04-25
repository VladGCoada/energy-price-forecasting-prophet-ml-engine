from __future__ import annotations

import json
import math
import statistics
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from eu_energy_intelligence.intelligence.regime import RegimeDetector
from eu_energy_intelligence.intelligence.robust_forecasting import (
    ForecastRiskClassifier,
    SimpleRobustForecaster,
)

try:
    from sklearn.ensemble import IsolationForest
    from sklearn.preprocessing import StandardScaler
except ImportError:  # pragma: no cover - optional dependency
    IsolationForest = None  # type: ignore[assignment]
    StandardScaler = None  # type: ignore[assignment]

try:
    import pandas as pd
except ImportError:  # pragma: no cover - optional dependency
    pd = None  # type: ignore[assignment]

try:
    from prophet import Prophet
except ImportError:  # pragma: no cover - optional dependency
    Prophet = None  # type: ignore[assignment]


def _utc_now() -> str:
    return datetime.now(UTC).isoformat()


def _read_json_rows(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json_rows(path: Path, rows: Any) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(rows, indent=2, default=str), encoding="utf-8")
    return str(path)


def _parse_ts(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def _row_zone(row: dict[str, Any]) -> str:
    return str(row.get("zone") or row.get("country_code") or "UNKNOWN")


def _row_ts(row: dict[str, Any]) -> str | None:
    return (
        row.get("timestamp_utc")
        or row.get("event_timestamp_utc")
        or row.get("timestamp")
    )


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _median_abs_deviation(values: list[float]) -> float:
    if not values:
        return 0.0
    median = statistics.median(values)
    return statistics.median(abs(v - median) for v in values)


def _mean(values: list[float]) -> float:
    return statistics.mean(values) if values else 0.0


def _stddev(values: list[float]) -> float:
    return statistics.pstdev(values) if len(values) > 1 else 0.0


def _isoformat(dt: datetime | None) -> str:
    return dt.isoformat() if dt is not None else "UNKNOWN_TS"


def _normalize_price_rows(rows: list[dict[str, Any]], zone: str) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for row in rows:
        if _row_zone(row) != zone:
            continue
        price = _safe_float(row.get("price_eur_mwh"))
        ts = _parse_ts(_row_ts(row))
        if price is None or ts is None:
            continue
        out.append(
            {
                "zone": zone,
                "timestamp_utc": ts,
                "price_eur_mwh": price,
                "event_date": ts.date().isoformat(),
            }
        )
    return sorted(out, key=lambda row: row["timestamp_utc"])


def _normalize_generation_rows(rows: list[dict[str, Any]], zone: str) -> dict[str, dict[str, Any]]:
    aggregated: dict[str, dict[str, Any]] = {}
    for row in rows:
        if _row_zone(row) != zone:
            continue
        ts = _parse_ts(_row_ts(row))
        if ts is None:
            continue
        quantity = (
            _safe_float(row.get("generation_mw"))
            or _safe_float(row.get("generation_mwh"))
            or _safe_float(row.get("quantity"))
        )
        if quantity is None:
            continue
        key = _isoformat(ts)
        current = aggregated.setdefault(
            key,
            {
                "zone": zone,
                "timestamp_utc": ts,
                "generation_mw": 0.0,
                "generation_row_count": 0,
            },
        )
        current["generation_mw"] += quantity
        current["generation_row_count"] += 1
    return aggregated


def _normalize_load_rows(rows: list[dict[str, Any]], zone: str) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for row in rows:
        if _row_zone(row) != zone:
            continue
        ts = _parse_ts(_row_ts(row))
        if ts is None:
            continue
        actual = _safe_float(row.get("actual_load_mw"))
        forecast = _safe_float(row.get("forecast_load_mw"))
        key = _isoformat(ts)
        out[key] = {
            "zone": zone,
            "timestamp_utc": ts,
            "actual_load_mw": actual,
            "forecast_load_mw": forecast,
            "abs_forecast_error_mw": (
                abs(actual - forecast) if actual is not None and forecast is not None else 0.0
            ),
        }
    return out


def build_local_feature_rows(processed_base_dir: str, zone: str) -> list[dict[str, Any]]:
    base = Path(processed_base_dir)
    price_rows = _normalize_price_rows(
        _read_json_rows(base / "silver" / "prices" / "records.json"),
        zone,
    )
    generation_rows = _normalize_generation_rows(
        _read_json_rows(base / "silver" / "generation" / "records.json"),
        zone,
    )
    load_rows = _normalize_load_rows(
        _read_json_rows(base / "silver" / "load" / "records.json"),
        zone,
    )

    anchor_keys = {row["timestamp_utc"].isoformat() for row in price_rows}
    if not anchor_keys:
        anchor_keys = set(generation_rows) | set(load_rows)
    combined: list[dict[str, Any]] = []

    sorted_keys = sorted(anchor_keys)
    price_history: list[float] = []
    generation_history: list[float] = []
    for key in sorted_keys:
        ts = _parse_ts(key)
        if ts is None:
            continue
        price_row = next((row for row in price_rows if row["timestamp_utc"].isoformat() == key), None)
        generation_row = generation_rows.get(key)
        load_row = load_rows.get(key)
        price = price_row["price_eur_mwh"] if price_row else None
        generation = generation_row["generation_mw"] if generation_row else None

        trailing_prices = price_history[-96:]
        trailing_generation = generation_history[-96:]
        price_avg = _mean(trailing_prices)
        price_std = _stddev(trailing_prices)
        generation_avg = _mean(trailing_generation)
        generation_std = _stddev(trailing_generation)

        row = {
            "zone": zone,
            "timestamp_utc": key,
            "event_date": ts.date().isoformat(),
            "price_eur_mwh": price if price is not None else 0.0,
            "price_24h_avg": price_avg,
            "price_24h_stddev": price_std,
            "price_z_score": (
                (price - price_avg) / price_std if price is not None and price_std > 0 else 0.0
            ),
            "price_spike_flag": bool(price is not None and price > 200.0),
            "negative_price_flag": bool(price is not None and price < 0.0),
            "generation_mw": generation if generation is not None else 0.0,
            "generation_24h_avg": generation_avg,
            "generation_24h_stddev": generation_std,
            "generation_volatility_ix": (
                generation_std / generation_avg if generation_avg > 0 else 0.0
            ),
            "actual_load_mw": load_row["actual_load_mw"] if load_row else 0.0,
            "forecast_load_mw": load_row["forecast_load_mw"] if load_row else 0.0,
            "abs_forecast_error_mw": load_row["abs_forecast_error_mw"] if load_row else 0.0,
        }
        combined.append(row)

        if price is not None:
            price_history.append(price)
        if generation is not None:
            generation_history.append(generation)

    return combined


def available_feature_columns(rows: list[dict[str, Any]]) -> list[str]:
    candidates = [
        "price_eur_mwh",
        "price_z_score",
        "generation_mw",
        "generation_volatility_ix",
        "abs_forecast_error_mw",
    ]
    return [
        column
        for column in candidates
        if any(abs(float(row.get(column, 0.0) or 0.0)) > 0 for row in rows)
    ] or ["price_eur_mwh"]


@dataclass(slots=True)
class LocalMlArtifacts:
    feature_rows: list[dict[str, Any]]
    anomaly_rows: list[dict[str, Any]]
    forecast_rows: list[dict[str, Any]]
    forecast_risk_rows: list[dict[str, Any]]
    metadata: dict[str, Any]


class LocalAnomalyModel:
    def __init__(self, feature_cols: list[str]) -> None:
        self.feature_cols = feature_cols
        self.model_type = (
            "isolation_forest"
            if IsolationForest is not None and StandardScaler is not None
            else "robust_zscore_fallback"
        )
        self.scaler: Any = None
        self.model: Any = None
        self.feature_stats: dict[str, dict[str, float]] = {}

    def fit(self, rows: list[dict[str, Any]]) -> dict[str, Any]:
        matrix = [
            [float(row.get(column, 0.0) or 0.0) for column in self.feature_cols]
            for row in rows
        ]
        if not matrix:
            raise ValueError("Cannot train anomaly model on empty rows")

        for idx, column in enumerate(self.feature_cols):
            values = [row[idx] for row in matrix]
            self.feature_stats[column] = {
                "mean": _mean(values),
                "stddev": _stddev(values),
                "median": statistics.median(values),
                "mad": _median_abs_deviation(values),
            }

        if self.model_type == "isolation_forest":
            self.scaler = StandardScaler()
            scaled = self.scaler.fit_transform(matrix)
            self.model = IsolationForest(contamination=0.1, random_state=42)
            self.model.fit(scaled)

        return {
            "model_type": self.model_type,
            "feature_cols": self.feature_cols,
            "training_rows": len(rows),
            "trained_at": _utc_now(),
            "feature_stats": self.feature_stats,
        }

    def score(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        if not rows:
            return []

        if self.model_type == "isolation_forest":
            matrix = [
                [float(row.get(column, 0.0) or 0.0) for column in self.feature_cols]
                for row in rows
            ]
            scaled = self.scaler.transform(matrix)
            raw_scores = self.model.score_samples(scaled)
            score_min = min(raw_scores)
            score_max = max(raw_scores)
            denom = (score_max - score_min) or 1.0
            normalized = [1 - ((score - score_min) / denom) for score in raw_scores]
        else:
            normalized = []
            for row in rows:
                component_scores: list[float] = []
                for column in self.feature_cols:
                    value = float(row.get(column, 0.0) or 0.0)
                    stats = self.feature_stats[column]
                    scale = max(stats["stddev"], 1.4826 * stats["mad"], 1e-9)
                    z_score = abs(value - stats["median"]) / scale
                    component_scores.append(min(z_score / 3.0, 1.0))
                normalized.append(round(_mean(component_scores), 6))

        detector = RegimeDetector()
        scored: list[dict[str, Any]] = []
        for row, anomaly_score in zip(rows, normalized, strict=False):
            new_row = dict(row)
            new_row["anomaly_score"] = round(float(anomaly_score), 6)
            new_row["is_anomaly"] = float(anomaly_score) >= 0.6
            new_row["regime_label"] = detector.classify_point(
                float(row.get("price_eur_mwh", 0.0) or 0.0),
                float(anomaly_score),
            )
            new_row["model_type"] = self.model_type
            scored.append(new_row)
        return scored


def run_local_price_forecast(
    rows: list[dict[str, Any]],
    zone: str,
    horizon_intervals: int = 96,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    price_series = [
        row
        for row in rows
        if _safe_float(row.get("price_eur_mwh")) is not None
        and float(row.get("price_eur_mwh", 0.0) or 0.0) != 0.0
    ]
    generation_series = [
        row
        for row in rows
        if _safe_float(row.get("generation_mw")) is not None
        and float(row.get("generation_mw", 0.0) or 0.0) != 0.0
    ]

    if price_series:
        target = "price_eur_mwh"
        series = price_series
        model_name = "price"
    else:
        target = "generation_mw"
        series = generation_series
        model_name = "generation"

    timestamps = [str(row["timestamp_utc"]) for row in series]
    values = [float(row.get(target, 0.0) or 0.0) for row in series]

    if target == "price_eur_mwh" and Prophet is not None and pd is not None and len(values) >= 100:
        ds_values = [pd.Timestamp(ts).tz_convert("UTC").tz_localize(None) for ts in timestamps]
        df = pd.DataFrame({"ds": ds_values, "y": values})
        model = Prophet(yearly_seasonality=True, weekly_seasonality=True, daily_seasonality=True)
        model.fit(df)
        future = model.make_future_dataframe(periods=horizon_intervals, freq="15min")
        forecast = model.predict(future).tail(horizon_intervals)
        forecast_rows = [
            {
                "zone": zone,
                "timestamp_utc": str(row.ds.to_pydatetime().replace(tzinfo=UTC).isoformat()),
                "yhat": round(float(row.yhat), 4),
                "yhat_lower": round(float(row.yhat_lower), 4),
                "yhat_upper": round(float(row.yhat_upper), 4),
                "model_name": "prophet",
                "model_version": "local-prophet-v1",
                "generated_at_utc": _utc_now(),
            }
            for row in forecast.itertuples()
        ]
        return forecast_rows, {
            "forecast_model": "prophet",
            "training_rows": len(values),
            "target": target,
        }

    fallback = SimpleRobustForecaster(horizon_intervals=horizon_intervals)
    forecast_rows = fallback.forecast_series(zone, timestamps, values or [0.0])
    for row in forecast_rows:
        row["forecast_target"] = target
        row["model_name"] = f"{model_name}_simple_robust_seasonal_forecaster"
    return forecast_rows, {
        "forecast_model": "simple_robust_seasonal_forecaster",
        "training_rows": len(values),
        "target": target,
    }


def run_local_ml_pipeline(
    processed_base_dir: str = "data/processed",
    zone: str = "NL",
    horizon_intervals: int = 96,
) -> dict[str, Any]:
    feature_rows = build_local_feature_rows(processed_base_dir, zone)
    if not feature_rows:
        raise ValueError(
            f"No usable local ML feature rows found for zone={zone} in {processed_base_dir}"
        )

    feature_cols = available_feature_columns(feature_rows)
    anomaly_model = LocalAnomalyModel(feature_cols)
    training_summary = anomaly_model.fit(feature_rows)
    anomaly_rows = anomaly_model.score(feature_rows)
    forecast_rows, forecast_summary = run_local_price_forecast(
        feature_rows,
        zone=zone,
        horizon_intervals=horizon_intervals,
    )
    forecast_risk_rows = (
        ForecastRiskClassifier().classify(forecast_rows)
        if forecast_summary["target"] == "price_eur_mwh"
        else forecast_rows
    )

    base = Path(processed_base_dir)
    output_dir = base / "ml"
    target_slug = (
        forecast_summary["target"]
        .replace("_mwh", "")
        .replace("_mw", "")
    )
    output_paths = {
        "feature_path": _write_json_rows(output_dir / "features" / f"{zone.lower()}_feature_rows.json", feature_rows),
        "anomaly_scores_path": _write_json_rows(output_dir / "anomaly_scores" / f"{zone.lower()}_anomaly_scores.json", anomaly_rows),
        "forecast_path": _write_json_rows(output_dir / "forecasts" / f"{zone.lower()}_{target_slug}_forecast.json", forecast_rows),
        "forecast_risk_path": _write_json_rows(output_dir / "forecast_risks" / f"{zone.lower()}_{target_slug}_forecast_risks.json", forecast_risk_rows),
    }
    metadata = {
        "zone": zone,
        "processed_base_dir": processed_base_dir,
        "feature_cols": feature_cols,
        "feature_row_count": len(feature_rows),
        "anomaly_row_count": len(anomaly_rows),
        "forecast_row_count": len(forecast_rows),
        "anomaly_training": training_summary,
        "forecast_training": forecast_summary,
        "generated_at_utc": _utc_now(),
        **output_paths,
    }
    metadata_path = _write_json_rows(output_dir / "runs" / f"{zone.lower()}_ml_run_summary.json", metadata)
    metadata["run_summary_path"] = metadata_path
    return metadata
