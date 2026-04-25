import json
from pathlib import Path

from eu_energy_intelligence.intelligence.local_ml import (
    available_feature_columns,
    build_local_feature_rows,
    run_local_ml_pipeline,
)


def _write(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(rows), encoding="utf-8")


def test_build_local_feature_rows_joins_price_generation_and_load(tmp_path: Path) -> None:
    base = tmp_path
    _write(
        base / "silver" / "prices" / "records.json",
        [
            {"zone": "NL", "timestamp_utc": "2024-01-01T00:00:00+00:00", "price_eur_mwh": 50.0},
            {"zone": "NL", "timestamp_utc": "2024-01-01T00:15:00+00:00", "price_eur_mwh": 60.0},
        ],
    )
    _write(
        base / "silver" / "generation" / "records.json",
        [
            {"country_code": "NL", "event_timestamp_utc": "2024-01-01T00:00:00+00:00", "quantity": 100.0},
            {"country_code": "NL", "event_timestamp_utc": "2024-01-01T00:15:00+00:00", "quantity": 120.0},
        ],
    )
    _write(
        base / "silver" / "load" / "records.json",
        [
            {
                "zone": "NL",
                "timestamp_utc": "2024-01-01T00:00:00+00:00",
                "actual_load_mw": 1000.0,
                "forecast_load_mw": 980.0,
            }
        ],
    )

    rows = build_local_feature_rows(str(base), "NL")
    assert len(rows) == 2
    assert rows[0]["generation_mw"] == 100.0
    assert rows[0]["abs_forecast_error_mw"] == 20.0
    assert rows[1]["price_eur_mwh"] == 60.0


def test_available_feature_columns_prefers_populated_features() -> None:
    cols = available_feature_columns(
        [{"price_eur_mwh": 50.0, "price_z_score": 0.2, "generation_mw": 0.0}]
    )
    assert "price_eur_mwh" in cols
    assert "price_z_score" in cols


def test_run_local_ml_pipeline_writes_outputs(tmp_path: Path) -> None:
    base = tmp_path
    price_rows = []
    generation_rows = []
    load_rows = []
    for idx in range(120):
        minute = idx * 15
        hour = (minute // 60) % 24
        day = 1 + (minute // (24 * 60))
        minute_of_hour = minute % 60
        ts = f"2024-01-{day:02d}T{hour:02d}:{minute_of_hour:02d}:00+00:00"
        price_rows.append({"zone": "NL", "timestamp_utc": ts, "price_eur_mwh": 50.0 + (idx % 12)})
        generation_rows.append({"country_code": "NL", "event_timestamp_utc": ts, "quantity": 100.0 + (idx % 8)})
        load_rows.append(
            {
                "zone": "NL",
                "timestamp_utc": ts,
                "actual_load_mw": 1000.0 + idx,
                "forecast_load_mw": 990.0 + idx,
            }
        )
    _write(base / "silver" / "prices" / "records.json", price_rows)
    _write(base / "silver" / "generation" / "records.json", generation_rows)
    _write(base / "silver" / "load" / "records.json", load_rows)

    result = run_local_ml_pipeline(str(base), zone="NL", horizon_intervals=8)

    assert result["feature_row_count"] == 120
    assert result["anomaly_row_count"] == 120
    assert result["forecast_row_count"] == 8
    assert Path(result["run_summary_path"]).exists()
