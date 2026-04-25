from datetime import date

import pytest

from eu_energy_intelligence.compliance import DoraIncidentClassifier, PiiTagger
from eu_energy_intelligence.ingestion import FLOW_CORRIDORS, RENEWABLE_PSR_TYPES, ZONE_EIC
from eu_energy_intelligence.ingestion.entsoe_client import ProductionEntsoeClient
from eu_energy_intelligence.intelligence import RegimeDetector
from eu_energy_intelligence.orchestration import PipelineRunner
from eu_energy_intelligence.quality import DQ_RULE_REGISTRY, DQCriticalFailure, DQValidator
from eu_energy_intelligence.settings import PlatformConfig
from eu_energy_intelligence.tasks import BaseTask


class DummyTask(BaseTask):
    def run(self):
        return self.empty_metrics()


def test_platform_config_defaults_and_schema_map(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("EMIT_CATALOG", raising=False)
    config = PlatformConfig()

    assert config.catalog == "emit_dev"
    assert config.schemas["bronze"] == "bronze"
    assert "DK-1" in config.bidding_zones
    assert "RO" in config.bidding_zones


def test_platform_config_env_override(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("EMIT_CATALOG", "emit_test")
    monkeypatch.setenv("EMIT_BIDDING_ZONES", "NL,DE")

    config = PlatformConfig()

    assert config.catalog == "emit_test"
    assert config.bidding_zones == ["NL", "DE"]


def test_entsoe_client_zone_metadata_and_resolution() -> None:
    assert ZONE_EIC["NL"] == "10YNL----------L"
    assert ZONE_EIC["RO"] == "10YRO-TEL------P"
    assert ("DE", "DK-1") in FLOW_CORRIDORS
    assert ("RO", "HU") in FLOW_CORRIDORS
    assert "Other renewable" in RENEWABLE_PSR_TYPES

    class Delta:
        def __init__(self, seconds: int) -> None:
            self.seconds = seconds

        def total_seconds(self) -> int:
            return self.seconds

    minutes = ProductionEntsoeClient.infer_resolution_minutes([0, Delta(900)])
    assert minutes == 15


def test_entsoe_client_unknown_zone_raises() -> None:
    with pytest.raises(ValueError):
        ProductionEntsoeClient.eic("XX")


def test_pd_timestamp_helper_requires_pandas_or_returns_two_values() -> None:
    try:
        start, end = ProductionEntsoeClient.pd_timestamps(date(2024, 1, 1), date(2024, 1, 2))
    except ImportError:
        pytest.skip("pandas not installed in current environment")

    assert start.tz is not None
    assert end.tz is not None


def test_dq_rule_registry_has_expected_sets() -> None:
    assert set(DQ_RULE_REGISTRY) == {
        "PRICE_RULES",
        "GENERATION_RULES",
        "LOAD_RULES",
        "FLOW_RULES",
    }
    assert all("rule_name" in rule for rules in DQ_RULE_REGISTRY.values() for rule in rules)


def test_dq_validator_passes_clean_rows() -> None:
    validator = DQValidator()

    rows, pass_rate = validator.validate_records(
        [{"zone": "NL", "price_eur_mwh": 85.0}],
        "PRICE_RULES",
        "emit_dev.silver.silver_prices",
        "run-1",
    )

    assert rows == [{"zone": "NL", "price_eur_mwh": 85.0}]
    assert pass_rate == 1.0


def test_dq_validator_rejects_invalid_zone() -> None:
    validator = DQValidator()

    with pytest.raises(DQCriticalFailure):
        validator.validate_records(
            [{"zone": "XX", "price_eur_mwh": 85.0}],
            "PRICE_RULES",
            "emit_dev.silver.silver_prices",
            "run-2",
        )


def test_dora_classifier_assigns_major_and_significant() -> None:
    classifier = DoraIncidentClassifier()

    major = classifier.classify("run-major", "boom", duration_minutes=300)
    significant = classifier.classify(
        "run-sig",
        "boom",
        duration_minutes=10,
        is_cross_border=True,
    )

    assert major["severity"] == "MAJOR"
    assert significant["severity"] == "SIGNIFICANT"


def test_pii_tagger_detects_pattern_matches() -> None:
    tagger = PiiTagger()
    tagged = tagger.detect_columns(["customer_name", "meter_id", "billing_email", "zone"])

    assert tagged == ["customer_name", "billing_email"]


def test_base_task_and_pipeline_runner_basics() -> None:
    task = DummyTask()
    runner = PipelineRunner()

    assert task.table("silver", "prices") == f"{task.config.catalog}.silver.prices"
    assert task.run() == {"rows_read": 0, "rows_written": 0, "rows_quarantined": 0}
    assert runner.run() == {"rows_read": 0, "rows_written": 0, "rows_quarantined": 0}


def test_regime_detector_labels_match_extension_logic() -> None:
    detector = RegimeDetector()

    assert detector.classify_point(-5.0) == "NEGATIVE"
    assert detector.classify_point(250.0) == "SPIKE"
    assert detector.classify_point(50.0, anomaly_score=0.8) == "STRESS"
    assert detector.classify_point(50.0, anomaly_score=0.1) == "NORMAL"
