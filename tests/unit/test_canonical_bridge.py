from eu_energy_intelligence.canonical_bridge import (
    DQValidator,
    ExecutionMode,
    GoldRenewableStabilityTask,
    PipelineRunner,
    PlatformConfig,
    PlatinumCarbonAdjustedPricesTask,
    ProductionEntsoeClient,
)


def test_canonical_bridge_prefers_modern_runtime_surfaces() -> None:
    assert PlatformConfig.__module__.endswith("production_2030")
    assert ProductionEntsoeClient.__module__.endswith("production_2030")
    assert PipelineRunner.__module__.endswith("production_2030")


def test_canonical_bridge_keeps_legacy_only_surfaces_available() -> None:
    assert DQValidator is not None
    assert DQValidator.__module__.endswith("production_extension")


def test_canonical_bridge_exposes_richer_gold_and_platinum_tasks() -> None:
    assert GoldRenewableStabilityTask.__module__.endswith("production_2030")
    assert PlatinumCarbonAdjustedPricesTask.__module__.endswith("production_2030")
    assert ExecutionMode.STREAMING.value == "streaming"
