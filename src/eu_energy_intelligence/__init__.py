"""EU Energy Intelligence Platform package."""

from eu_energy_intelligence.backfill import BackfillPlanner, BackfillWindow
from eu_energy_intelligence.contracts import ContractValidator, FieldContract, TableContract
from eu_energy_intelligence.reliability import CheckpointStore, ManifestStore, RetryPolicy
from eu_energy_intelligence.runtime import Layer, RunContext, Status, TaskResult
from eu_energy_intelligence.settings import PlatformConfig
from eu_energy_intelligence.scaffold import generate_production_scaffold

__all__ = [
    "BackfillPlanner",
    "BackfillWindow",
    "CheckpointStore",
    "ContractValidator",
    "FieldContract",
    "Layer",
    "ManifestStore",
    "PlatformConfig",
    "RetryPolicy",
    "RunContext",
    "Status",
    "TableContract",
    "TaskResult",
    "__version__",
    "generate_production_scaffold",
]

__version__ = "0.1.0"
