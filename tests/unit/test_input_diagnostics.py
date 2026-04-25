import json
from pathlib import Path

from eu_energy_intelligence.intelligence import summarize_processed_inputs


def test_summarize_processed_inputs_reports_missing_datasets(tmp_path: Path) -> None:
    summary = summarize_processed_inputs(str(tmp_path))
    assert summary["counts"]["silver_prices"] == 0
    assert summary["usable_for_market_stress"] is False
    assert summary["warnings"]


def test_summarize_processed_inputs_detects_flow_corridors(tmp_path: Path) -> None:
    flow_dir = tmp_path / "silver" / "flows"
    flow_dir.mkdir(parents=True)
    (flow_dir / "records.json").write_text(
        json.dumps(
            [
                {"zone_from": "RO", "zone_to": "HU", "flow_mw": 120.0},
                {"zone_from": "RO", "zone_to": "HU", "flow_mw": 130.0},
            ]
        ),
        encoding="utf-8",
    )

    summary = summarize_processed_inputs(str(tmp_path))
    assert summary["counts"]["silver_flows"] == 2
    assert summary["flow_corridors"]["RO->HU"] == 2
