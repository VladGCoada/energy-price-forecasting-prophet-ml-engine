from eu_energy_intelligence.backfill import BackfillPlanner
from eu_energy_intelligence.contracts import CONTRACTS, TableContract
from eu_energy_intelligence.features import FeatureBuilder
from eu_energy_intelligence.intelligence import PriceForecaster
from eu_energy_intelligence.observability import ObservabilityReporter
from eu_energy_intelligence.platinum import (
    PlatinumArbitrageOptimizerTask,
    PlatinumCarbonAdjustedPricesTask,
)
from eu_energy_intelligence.reliability import CheckpointStore, ManifestStore, RetryPolicy
from eu_energy_intelligence.runtime import Layer, RunContext, Status, TaskResult


def test_run_context_and_task_result_workflows() -> None:
    context = RunContext.create("emit_pipeline", Layer.GOLD, "dev")
    result = TaskResult.empty("gold_task").finish(Status.SUCCESS)

    assert context.pipeline_name == "emit_pipeline"
    assert result.status == Status.SUCCESS
    assert "task_name" in result.to_dict()


def test_manifest_and_checkpoint_store_roundtrip(tmp_path) -> None:
    manifests = ManifestStore(str(tmp_path / "manifests"))
    checkpoints = CheckpointStore(str(tmp_path / "checkpoints"))

    manifest_path = manifests.write_task(TaskResult.empty("demo").finish(Status.SUCCESS))
    checkpoints.mark_success("prices", "NL", "2024-01-01", 10)

    assert manifest_path.endswith(".json")
    assert checkpoints.watermark("prices", "NL", "2020-01-01") == "2024-01-01"


def test_retry_policy_runs_callable() -> None:
    policy = RetryPolicy(attempts=2, backoff_seconds=0.0)
    assert policy.run(lambda: "ok") == "ok"


def test_backfill_planner_summary_returns_gap_payload(tmp_path) -> None:
    planner = BackfillPlanner(CheckpointStore(str(tmp_path / "checkpoints")))
    summary = planner.summary(["prices"], ["NL"], "2024-01-03", "2024-01-01")

    assert summary["gap_count"] == 1


def test_contract_registry_contains_table_contracts() -> None:
    assert isinstance(CONTRACTS["gold_renewable_stability"], TableContract)
    assert "event_date" in CONTRACTS["gold_renewable_stability"].column_names()


def test_advanced_surfaces_are_importable() -> None:
    assert FeatureBuilder.__name__ == "FeatureBuilder"
    assert PriceForecaster.__name__ == "PriceForecaster"
    assert ObservabilityReporter.__name__ == "ObservabilityReporter"
    assert PlatinumCarbonAdjustedPricesTask.__name__ == "PlatinumCarbonAdjustedPricesTask"
    assert PlatinumArbitrageOptimizerTask.__name__ == "PlatinumArbitrageOptimizerTask"
