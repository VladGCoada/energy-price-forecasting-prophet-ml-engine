from eu_energy_intelligence.gold import build_renewable_stability
from eu_energy_intelligence.silver import build_generation_silver


def test_build_generation_silver_splits_invalid_rows() -> None:
    valid_rows, quarantine_rows = build_generation_silver(
        [
            {"country_code": "DE", "position": 1, "source_file": "a.xml", "quantity": 10.0},
            {"country_code": "DE", "position": 1, "source_file": "a.xml", "quantity": 10.0},
            {"country_code": "DE", "position": 2, "source_file": "a.xml", "quantity": -5.0},
            {"country_code": "DE", "position": 3, "source_file": "a.xml", "quantity": None},
        ]
    )

    assert len(valid_rows) == 1
    assert len(quarantine_rows) == 2


def test_build_renewable_stability_aggregates_rows_by_country() -> None:
    result = build_renewable_stability(
        [
            {"country_code": "DE", "quantity": 10.0},
            {"country_code": "DE", "quantity": 20.0},
            {"country_code": "NL", "quantity": 5.0},
        ]
    )

    assert result == [
        {
            "country_code": "DE",
            "event_date": "UNKNOWN_DATE",
            "total_generation": 30.0,
            "avg_generation": 15.0,
            "max_generation": 20.0,
            "min_generation": 10.0,
            "volatility_index": 10.0,
            "interval_count": 2.0,
        },
        {
            "country_code": "NL",
            "event_date": "UNKNOWN_DATE",
            "total_generation": 5.0,
            "avg_generation": 5.0,
            "max_generation": 5.0,
            "min_generation": 5.0,
            "volatility_index": 0.0,
            "interval_count": 1.0,
        },
    ]
