from __future__ import annotations

import os
from datetime import date, timedelta
from typing import Any

import requests

from eu_energy_intelligence.constants import ENTSOE_BASE_URL
from eu_energy_intelligence.logging_config import get_logger

logger = get_logger(__name__)

try:
    import pandas as pd
except ImportError:  # pragma: no cover
    pd = None

try:
    from entsoe import EntsoePandasClient
except ImportError:  # pragma: no cover
    EntsoePandasClient = None


ZONE_EIC: dict[str, str] = {
    "NL": "10YNL----------L",
    "DE": "10Y1001A1001A83F",
    "DK-1": "10YDK-1--------W",
    "DK-2": "10YDK-2--------M",
    "FR": "10YFR-RTE------C",
    "BE": "10YBE----------2",
    "RO": "10YRO-TEL------P",
    "HU": "10YHU-MAVIR----U",
}

FLOW_CORRIDORS: list[tuple[str, str]] = [
    ("NL", "DE"),
    ("DE", "NL"),
    ("DE", "DK-1"),
    ("DK-1", "DE"),
    ("DK-1", "DK-2"),
    ("DK-2", "DK-1"),
    ("NL", "FR"),
    ("FR", "NL"),
    ("BE", "NL"),
    ("NL", "BE"),
    ("BE", "DE"),
    ("DE", "BE"),
    ("FR", "BE"),
    ("BE", "FR"),
    ("RO", "HU"),
    ("HU", "RO"),
]

RENEWABLE_PSR_TYPES: set[str] = {
    "Solar",
    "Wind Offshore",
    "Wind Onshore",
    "Hydro Water Reservoir",
    "Hydro Run-of-river and poundage",
    "Biomass",
    "Geothermal",
    "Other renewable",
}


class EntsoeClient:
    """Simple ENTSO-E Transparency Platform API client."""

    def __init__(self, api_key: str | None = None):
        self.api_key = api_key or os.getenv("ENTSOE_API_KEY")
        if not self.api_key:
            raise ValueError("ENTSOE_API_KEY not set")

    def get(self, params: dict[str, Any]) -> str:
        request_params = {**params, "securityToken": self.api_key}
        response = requests.get(ENTSOE_BASE_URL, params=request_params, timeout=60)
        response.raise_for_status()
        safe_params = {k: v for k, v in request_params.items() if k != "securityToken"}
        logger.info("Fetched ENTSO-E data with params=%s", safe_params)
        return response.text


class ProductionEntsoeClient:
    """Extension-style higher-level ENTSO-E client backed by `entsoe-py` when available."""

    def __init__(self, api_key: str | None = None) -> None:
        self.api_key = api_key or os.getenv("ENTSOE_API_KEY")
        if not self.api_key:
            raise ValueError("ENTSOE_API_KEY not set in environment or .env")
        if EntsoePandasClient is None:
            self._client = None
        else:
            self._client = EntsoePandasClient(api_key=self.api_key)

    @staticmethod
    def eic(zone: str) -> str:
        if zone not in ZONE_EIC:
            raise ValueError(f"Unknown zone '{zone}'. Valid zones: {list(ZONE_EIC)}")
        return ZONE_EIC[zone]

    @staticmethod
    def infer_resolution_minutes(index_like: Any) -> int:
        if len(index_like) > 1:
            try:
                delta = index_like[1] - index_like[0]
            except TypeError:
                delta = index_like[1]
            return int(delta.total_seconds() / 60)
        return 60

    @staticmethod
    def pd_timestamps(start: date, end: date):
        if pd is None:
            raise ImportError("pandas is required for ProductionEntsoeClient timestamp helpers")
        return (
            pd.Timestamp(start.isoformat(), tz="Europe/Brussels"),
            pd.Timestamp((end + timedelta(days=1)).isoformat(), tz="Europe/Brussels"),
        )
