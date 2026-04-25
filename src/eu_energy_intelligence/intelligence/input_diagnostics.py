from __future__ import annotations

import json
from collections import Counter
from pathlib import Path
from typing import Any


def _read_json_records(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    return json.loads(path.read_text(encoding="utf-8"))


def summarize_processed_inputs(processed_base_dir: str = "data/processed") -> dict[str, Any]:
    base = Path(processed_base_dir)
    dataset_paths = {
        "silver_prices": base / "silver" / "prices" / "records.json",
        "silver_generation": base / "silver" / "generation" / "records.json",
        "silver_load": base / "silver" / "load" / "records.json",
        "silver_flows": base / "silver" / "flows" / "records.json",
        "gold_renewable_stability": base / "gold" / "renewable_stability" / "records.json",
    }

    counts = {name: len(_read_json_records(path)) for name, path in dataset_paths.items()}
    available = {name: count > 0 for name, count in counts.items()}

    flow_rows = _read_json_records(dataset_paths["silver_flows"])
    flow_corridors = Counter(
        f"{row.get('zone_from')}->{row.get('zone_to')}"
        for row in flow_rows
        if row.get("zone_from") and row.get("zone_to")
    )

    warnings: list[str] = []
    if not available["silver_prices"]:
        warnings.append("No Silver prices found; market stress may fall back to synthetic price assumptions.")
    if not available["silver_load"]:
        warnings.append("No Silver load found; load-risk outputs may be underpowered.")
    if not available["silver_flows"]:
        warnings.append("No Silver flows found; import dependency and corridor-based intelligence will be limited.")
    if not available["gold_renewable_stability"]:
        warnings.append("No Gold renewable stability found; local intelligence runner has no daily generation baseline.")

    usable_for_market_stress = all(
        available[name]
        for name in ("silver_prices", "silver_generation", "silver_load", "gold_renewable_stability")
    )
    fully_populated_for_cross_border = usable_for_market_stress and available["silver_flows"]

    return {
        "processed_base_dir": str(base),
        "counts": counts,
        "available": available,
        "usable_for_market_stress": usable_for_market_stress,
        "fully_populated_for_cross_border": fully_populated_for_cross_border,
        "flow_corridors": dict(flow_corridors),
        "warnings": warnings,
    }

