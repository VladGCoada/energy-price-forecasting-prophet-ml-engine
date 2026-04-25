from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any
from xml.etree import ElementTree

from eu_energy_intelligence.ingestion.entsoe_client import EntsoeClient, ZONE_EIC

_POINT_TAGS = (
    "{urn:iec62325.351:tc57wg16:451-3:publicationdocument:7:0}Point",
    "{urn:iec62325.351:tc57wg16:451-3:publicationdocument:7:3}Point",
    "{urn:iec62325.351:tc57wg16:451-6:generationloaddocument:3:0}Point",
)


@dataclass(slots=True)
class ProbeResult:
    dataset: str
    status_code: int
    point_count: int
    has_data: bool
    period_start: str
    period_end: str
    zone: str
    flow_partner: str | None = None
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "dataset": self.dataset,
            "status_code": self.status_code,
            "point_count": self.point_count,
            "has_data": self.has_data,
            "period_start": self.period_start,
            "period_end": self.period_end,
            "zone": self.zone,
            "flow_partner": self.flow_partner,
            "error": self.error,
        }


def count_xml_points(xml_text: str) -> int:
    try:
        root = ElementTree.fromstring(xml_text)
    except ElementTree.ParseError:
        return -1
    return sum(len(root.findall(f".//{tag}")) for tag in _POINT_TAGS)


def build_dataset_params(
    dataset: str,
    zone: str,
    period_start: str,
    period_end: str,
    flow_partner: str | None = None,
) -> dict[str, str]:
    zone_eic = ZONE_EIC[zone]
    if dataset == "prices":
        return {
            "documentType": "A44",
            "in_Domain": zone_eic,
            "out_Domain": zone_eic,
            "periodStart": period_start,
            "periodEnd": period_end,
        }
    if dataset == "load":
        return {
            "documentType": "A65",
            "processType": "A16",
            "in_Domain": zone_eic,
            "outBiddingZone_Domain": zone_eic,
            "periodStart": period_start,
            "periodEnd": period_end,
        }
    if dataset == "generation":
        return {
            "documentType": "A75",
            "processType": "A16",
            "in_Domain": zone_eic,
            "periodStart": period_start,
            "periodEnd": period_end,
        }
    if dataset == "flows":
        if flow_partner is None:
            raise ValueError("flow_partner is required for flows probes")
        return {
            "documentType": "A11",
            "in_Domain": zone_eic,
            "out_Domain": ZONE_EIC[flow_partner],
            "periodStart": period_start,
            "periodEnd": period_end,
        }
    raise ValueError(f"Unsupported dataset '{dataset}'")


class EntsoeOverlapProbe:
    def __init__(self, client: EntsoeClient | None = None) -> None:
        self.client = client or EntsoeClient()

    def probe_dataset(
        self,
        dataset: str,
        zone: str,
        period_start: str,
        period_end: str,
        flow_partner: str | None = None,
    ) -> ProbeResult:
        params = build_dataset_params(dataset, zone, period_start, period_end, flow_partner)
        try:
            xml_text = self.client.get(params)
        except Exception as exc:  # pragma: no cover - network failure path
            return ProbeResult(
                dataset=dataset,
                status_code=0,
                point_count=0,
                has_data=False,
                period_start=period_start,
                period_end=period_end,
                zone=zone,
                flow_partner=flow_partner,
                error=str(exc),
            )
        point_count = count_xml_points(xml_text)
        return ProbeResult(
            dataset=dataset,
            status_code=200,
            point_count=point_count,
            has_data=point_count > 0,
            period_start=period_start,
            period_end=period_end,
            zone=zone,
            flow_partner=flow_partner,
        )

    def probe_day(
        self,
        zone: str,
        day: date,
        flow_partner: str,
        datasets: tuple[str, ...] = ("prices", "load", "generation", "flows"),
    ) -> dict[str, Any]:
        period_start = day.strftime("%Y%m%d0000")
        period_end = (day + timedelta(days=1)).strftime("%Y%m%d0000")
        results = {
            dataset: self.probe_dataset(
                dataset=dataset,
                zone=zone,
                period_start=period_start,
                period_end=period_end,
                flow_partner=flow_partner if dataset == "flows" else None,
            ).to_dict()
            for dataset in datasets
        }
        return {
            "date": day.isoformat(),
            "zone": zone,
            "flow_partner": flow_partner,
            "datasets": results,
            "all_datasets_have_data": all(result["has_data"] for result in results.values()),
        }

    def probe_range(
        self,
        zone: str,
        flow_partner: str,
        start_date: str,
        end_date: str,
    ) -> dict[str, Any]:
        start = date.fromisoformat(start_date)
        end = date.fromisoformat(end_date)
        if end < start:
            raise ValueError("end_date must be on or after start_date")

        rows: list[dict[str, Any]] = []
        current = start
        while current <= end:
            rows.append(self.probe_day(zone=zone, day=current, flow_partner=flow_partner))
            current += timedelta(days=1)

        overlapping_days = [row["date"] for row in rows if row["all_datasets_have_data"]]
        point_summary: dict[str, int] = {}
        for dataset in ("prices", "load", "generation", "flows"):
            point_summary[dataset] = sum(
                int(row["datasets"][dataset]["point_count"])
                for row in rows
                if row["datasets"][dataset]["point_count"] > 0
            )

        return {
            "zone": zone,
            "flow_partner": flow_partner,
            "start_date": start_date,
            "end_date": end_date,
            "days_checked": len(rows),
            "overlap_days": overlapping_days,
            "overlap_day_count": len(overlapping_days),
            "point_summary": point_summary,
            "rows": rows,
        }


def normalize_zone(zone: str) -> str:
    normalized = zone.strip().upper()
    if normalized not in ZONE_EIC:
        raise ValueError(f"Unknown zone '{zone}'. Valid zones: {sorted(ZONE_EIC)}")
    return normalized


def default_flow_partner(zone: str) -> str:
    mapping = {
        "RO": "HU",
        "HU": "RO",
        "NL": "DE",
        "DE": "NL",
        "FR": "BE",
        "BE": "FR",
        "DK-1": "DE",
        "DK-2": "DK-1",
    }
    return mapping.get(zone, "DE")


def probe_entsoe_overlap(
    zone: str,
    start_date: str,
    end_date: str,
    flow_partner: str | None = None,
    client: EntsoeClient | None = None,
) -> dict[str, Any]:
    normalized_zone = normalize_zone(zone)
    normalized_partner = normalize_zone(flow_partner or default_flow_partner(normalized_zone))
    return EntsoeOverlapProbe(client).probe_range(
        zone=normalized_zone,
        flow_partner=normalized_partner,
        start_date=start_date,
        end_date=end_date,
    )

