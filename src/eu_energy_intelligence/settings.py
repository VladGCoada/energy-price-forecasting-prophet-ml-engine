from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml
from dotenv import load_dotenv

load_dotenv()

PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONF_DIR = PROJECT_ROOT / "conf"


@dataclass(slots=True)
class PlatformConfig:
    """Environment-backed platform configuration inspired by the extension baseline."""

    env: str = field(default_factory=lambda: os.getenv("EMIT_ENV", "dev"))
    catalog: str = field(default_factory=lambda: os.getenv("EMIT_CATALOG", "emit_dev"))
    bronze_schema: str = field(default_factory=lambda: os.getenv("EMIT_BRONZE_SCHEMA", "bronze"))
    silver_schema: str = field(default_factory=lambda: os.getenv("EMIT_SILVER_SCHEMA", "silver"))
    gold_schema: str = field(default_factory=lambda: os.getenv("EMIT_GOLD_SCHEMA", "gold"))
    platinum_schema: str = field(
        default_factory=lambda: os.getenv("EMIT_PLATINUM_SCHEMA", "platinum")
    )
    features_schema: str = field(
        default_factory=lambda: os.getenv("EMIT_FEATURES_SCHEMA", "features")
    )
    dq_schema: str = field(default_factory=lambda: os.getenv("EMIT_DQ_SCHEMA", "dq"))
    quality_schema: str = field(default_factory=lambda: os.getenv("EMIT_QUALITY_SCHEMA", "quality"))
    ops_schema: str = field(default_factory=lambda: os.getenv("EMIT_OPS_SCHEMA", "ops"))
    ml_schema: str = field(default_factory=lambda: os.getenv("EMIT_ML_SCHEMA", "ml"))
    serving_schema: str = field(default_factory=lambda: os.getenv("EMIT_SERVING_SCHEMA", "serving"))
    compliance_schema: str = field(
        default_factory=lambda: os.getenv("EMIT_COMPLIANCE_SCHEMA", "compliance")
    )
    entsoe_api_key: str = field(default_factory=lambda: os.getenv("ENTSOE_API_KEY", ""))
    entsoe_base_url: str = field(
        default_factory=lambda: os.getenv("EMIT_ENTSOE_BASE_URL", "https://web-api.tp.entsoe.eu/api")
    )
    ecb_base_url: str = field(
        default_factory=lambda: os.getenv(
            "EMIT_ECB_BASE_URL",
            "https://data-api.ecb.europa.eu/service/data",
        )
    )
    weather_base_url: str = field(
        default_factory=lambda: os.getenv(
            "EMIT_WEATHER_BASE_URL",
            "https://api.open-meteo.com/v1/forecast",
        )
    )
    carbon_base_url: str = field(
        default_factory=lambda: os.getenv(
            "EMIT_CARBON_BASE_URL",
            "https://api.carbonintensity.org.uk",
        )
    )
    checkpoint_base: str = field(
        default_factory=lambda: os.getenv("EMIT_CHECKPOINT_BASE", "/tmp/emit/checkpoints")
    )
    checkpoint_dir: str = field(
        default_factory=lambda: os.getenv("EMIT_CHECKPOINT_DIR", "./data/processed/checkpoints")
    )
    manifest_dir: str = field(
        default_factory=lambda: os.getenv("EMIT_MANIFEST_DIR", "./data/processed/manifests")
    )
    stream_trigger_seconds: int = field(
        default_factory=lambda: int(os.getenv("EMIT_STREAM_TRIGGER_SECONDS", "300"))
    )
    raw_data_dir: str = field(default_factory=lambda: os.getenv("RAW_DATA_DIR", "./data/raw"))
    processed_data_dir: str = field(
        default_factory=lambda: os.getenv("PROCESSED_DATA_DIR", "./data/processed")
    )
    mlflow_experiment: str = field(
        default_factory=lambda: os.getenv(
            "EMIT_MLFLOW_EXPERIMENT",
            "/experiments/emit_regime_detection",
        )
    )
    mlflow_model_name: str = field(
        default_factory=lambda: os.getenv("EMIT_MLFLOW_MODEL_NAME", "emit_anomaly_detector")
    )
    mlflow_model_name_regime: str = field(
        default_factory=lambda: os.getenv(
            "EMIT_MLFLOW_MODEL_NAME_REGIME",
            os.getenv("EMIT_MLFLOW_MODEL_NAME", "emit_regime_detector"),
        )
    )
    mlflow_model_name_forecast: str = field(
        default_factory=lambda: os.getenv("EMIT_MLFLOW_MODEL_NAME_FORECAST", "emit_price_forecast")
    )
    initial_load_date: str = field(
        default_factory=lambda: os.getenv("EMIT_INITIAL_LOAD_DATE", "2020-01-01")
    )
    dq_critical_threshold: float = field(
        default_factory=lambda: float(os.getenv("EMIT_DQ_CRITICAL_THRESHOLD", "0.80"))
    )
    dq_warn_threshold: float = field(
        default_factory=lambda: float(os.getenv("EMIT_DQ_WARN_THRESHOLD", "0.95"))
    )
    max_retries: int = field(default_factory=lambda: int(os.getenv("EMIT_MAX_RETRIES", "3")))
    retry_backoff_seconds: float = field(
        default_factory=lambda: float(os.getenv("EMIT_RETRY_BACKOFF_SECONDS", "2.0"))
    )
    use_delta: bool = field(
        default_factory=lambda: os.getenv("EMIT_USE_DELTA", "true").lower() == "true"
    )
    bidding_zones: list[str] = field(
        default_factory=lambda: os.getenv(
            "EMIT_BIDDING_ZONES",
            "NL,DE,DK-1,DK-2,FR,BE,RO",
        ).split(",")
    )

    @property
    def schemas(self) -> dict[str, str]:
        return {
            "bronze": self.bronze_schema,
            "silver": self.silver_schema,
            "gold": self.gold_schema,
            "platinum": self.platinum_schema,
            "features": self.features_schema,
            "dq": self.dq_schema,
            "quality": self.quality_schema,
            "ops": self.ops_schema,
            "ml": self.ml_schema,
            "serving": self.serving_schema,
            "compliance": self.compliance_schema,
        }


def get_env(name: str, default: Any = None) -> Any:
    """Read an environment variable."""
    return os.getenv(name, default)


def get_env_var(name: str, default: str | None = None) -> str | None:
    """Compatibility alias for environment lookups."""
    return os.getenv(name, default)


def resolve_config_path(environment_or_path: str = "dev") -> Path:
    """Resolve `dev` to `conf/dev.yml` or accept a direct YAML path."""
    candidate = Path(environment_or_path)
    if candidate.suffix in {".yml", ".yaml"}:
        return candidate if candidate.is_absolute() else (PROJECT_ROOT / candidate).resolve()
    return (CONF_DIR / f"{environment_or_path}.yml").resolve()


def load_config(environment_or_path: str = "dev") -> dict[str, Any]:
    """Load YAML config, falling back to environment-driven defaults when missing."""
    config_path = resolve_config_path(environment_or_path)
    if not config_path.exists():
        return {
            "env": environment_or_path,
            "catalog": get_env("EMIT_CATALOG", get_env("CATALOG", "energy_dev")),
            "schemas": {
                "bronze": get_env("EMIT_BRONZE_SCHEMA", get_env("BRONZE_SCHEMA", "bronze")),
                "silver": get_env("EMIT_SILVER_SCHEMA", get_env("SILVER_SCHEMA", "silver")),
                "gold": get_env("EMIT_GOLD_SCHEMA", get_env("GOLD_SCHEMA", "gold")),
                "ops": get_env("EMIT_OPS_SCHEMA", get_env("OPS_SCHEMA", "ops")),
                "dq": get_env("EMIT_DQ_SCHEMA", "dq"),
                "compliance": get_env("EMIT_COMPLIANCE_SCHEMA", "compliance"),
            },
            "countries": get_env("EMIT_BIDDING_ZONES", "NL,DE,DK-1,DK-2").split(","),
            "raw_data_dir": get_env("RAW_DATA_DIR", "./data/raw"),
            "processed_data_dir": get_env("PROCESSED_DATA_DIR", "./data/processed"),
        }

    with config_path.open("r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle) or {}

    if not isinstance(data, dict):
        raise ValueError(f"Config file must deserialize to a mapping: {config_path}")

    return data
