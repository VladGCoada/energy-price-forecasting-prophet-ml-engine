from __future__ import annotations

from typing import Any

from eu_energy_intelligence.extension_bridge import DQValidator as SparkDQValidator
from eu_energy_intelligence.quality.rules import DQ_RULE_REGISTRY
from eu_energy_intelligence.tasks.base import BaseTask


class DQCriticalFailure(Exception):
    """Raised when pass rate drops below the critical threshold."""

    def __init__(self, rule_set: str, pass_rate: float, table: str) -> None:
        super().__init__(
            f"DQ CRITICAL: {rule_set} pass_rate={pass_rate:.2%} < threshold on {table}"
        )
        self.rule_set = rule_set
        self.pass_rate = pass_rate
        self.table = table


class DQValidator(BaseTask):
    """Lightweight Python validator modeled after the Spark-based extension validator."""

    def run(self) -> dict[str, Any]:
        return self.empty_metrics()

    def validate(
        self,
        df: Any,
        rule_set_name: str,
        target_table: str,
        run_id: str,
    ) -> tuple[Any, float]:
        """Validate a Spark DataFrame through the production extension implementation."""
        validator = SparkDQValidator(self.config)
        return validator.validate(df, rule_set_name, target_table, run_id)

    def validate_records(
        self,
        rows: list[dict[str, Any]],
        rule_set_name: str,
        target_table: str,
        run_id: str,
    ) -> tuple[list[dict[str, Any]], float]:
        del run_id
        rules = DQ_RULE_REGISTRY.get(rule_set_name)
        if rules is None:
            raise ValueError(
                f"Unknown rule set '{rule_set_name}'. Available: {list(DQ_RULE_REGISTRY)}"
            )

        valid_rows = rows[:]
        for rule in rules:
            valid_rows = [row for row in valid_rows if self._passes_rule(row, rule)]

        total = len(rows)
        passed = len(valid_rows)
        pass_rate = passed / total if total else 1.0

        if pass_rate < self.config.dq_critical_threshold:
            raise DQCriticalFailure(rule_set_name, pass_rate, target_table)

        return valid_rows, pass_rate

    def _passes_rule(self, row: dict[str, Any], rule: dict[str, str]) -> bool:
        name = rule["rule_name"]
        if name == "price_not_null":
            return row.get("price_eur_mwh") is not None
        if name == "price_below_cap":
            value = row.get("price_eur_mwh")
            return value is not None and float(value) < 5000
        if name == "price_above_floor":
            value = row.get("price_eur_mwh")
            return value is not None and float(value) > -600
        if name == "zone_valid":
            return row.get("zone") in {"NL", "DE", "DK-1", "DK-2", "FR", "BE", "RO", "HU"}
        if name == "generation_not_negative":
            value = row.get("generation_mw")
            return value is not None and float(value) >= 0
        if name == "psr_type_not_null":
            return row.get("psr_type") is not None
        if name == "actual_load_non_negative":
            value = row.get("actual_load_mw")
            return value is None or float(value) >= 0
        if name == "not_both_null":
            return not (
                row.get("actual_load_mw") is None and row.get("forecast_load_mw") is None
            )
        if name == "flow_not_null":
            return row.get("flow_mw") is not None
        if name == "flow_in_physical_range":
            value = row.get("flow_mw")
            return value is not None and abs(float(value)) < 20000
        return True
