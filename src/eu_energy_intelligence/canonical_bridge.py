from __future__ import annotations

from importlib import import_module
from types import ModuleType


def _load(name: str) -> ModuleType:
    return import_module(name)


_EXT = _load("eu_energy_intelligence.production_extension")
_CONS = _load("eu_energy_intelligence.production_consolidated")
_Y2030 = _load("eu_energy_intelligence.production_2030")

# Backward-compatible module aliases for bridge consumers that still expect them.
_EXTENSION = _EXT
_CONSOLIDATED = _CONS
_INTELLIGENCE_2030 = _Y2030


def _prefer(*candidates: object) -> object:
    for candidate in candidates:
        if candidate is not None:
            return candidate
    raise AttributeError("No candidate available")


# Canonical runtime/config/task surfaces prefer the 2030 core.
PlatformConfig = _prefer(getattr(_Y2030, "PlatformConfig", None), getattr(_CONS, "PlatformConfig", None), getattr(_EXT, "PlatformConfig", None))
BaseTask = _prefer(getattr(_Y2030, "BaseTask", None), getattr(_CONS, "BaseTask", None), getattr(_EXT, "BaseTask", None))
ProductionEntsoeClient = _prefer(getattr(_Y2030, "ProductionEntsoeClient", None), getattr(_CONS, "ProductionEntsoeClient", None), getattr(_EXT, "ProductionEntsoeClient", None))

ENTSOE_PRICE_SCHEMA = _prefer(getattr(_Y2030, "ENTSOE_PRICE_SCHEMA", None), getattr(_EXT, "ENTSOE_PRICE_SCHEMA", None))
ENTSOE_GENERATION_SCHEMA = _prefer(getattr(_Y2030, "ENTSOE_GENERATION_SCHEMA", None), getattr(_EXT, "ENTSOE_GENERATION_SCHEMA", None))
ENTSOE_LOAD_SCHEMA = _prefer(getattr(_Y2030, "ENTSOE_LOAD_SCHEMA", None), getattr(_EXT, "ENTSOE_LOAD_SCHEMA", None))
ENTSOE_FLOW_SCHEMA = _prefer(getattr(_Y2030, "ENTSOE_FLOW_SCHEMA", None), getattr(_EXT, "ENTSOE_FLOW_SCHEMA", None))
DQ_STATS_SCHEMA = _prefer(getattr(_Y2030, "DQ_STATS_SCHEMA", None), getattr(_EXT, "DQ_STATS_SCHEMA", None))
PIPELINE_RUN_SCHEMA = _prefer(getattr(_Y2030, "PIPELINE_RUN_SCHEMA", None), getattr(_EXT, "PIPELINE_RUN_SCHEMA", None))
DORA_INCIDENT_SCHEMA = getattr(_EXT, "DORA_INCIDENT_SCHEMA", None)
GDPR_ERASURE_SCHEMA = getattr(_EXT, "GDPR_ERASURE_SCHEMA", None)
REGIME_SIGNAL_SCHEMA = _prefer(getattr(_Y2030, "REGIME_SIGNAL_SCHEMA", None), getattr(_EXT, "REGIME_SIGNAL_SCHEMA", None))

ZONE_EIC = _prefer(getattr(_Y2030, "ZONE_EIC", None), getattr(_EXT, "ZONE_EIC", None))
FLOW_CORRIDORS = _prefer(getattr(_Y2030, "FLOW_CORRIDORS", None), getattr(_EXT, "FLOW_CORRIDORS", None))
RENEWABLE_PSR_TYPES = _prefer(getattr(_Y2030, "RENEWABLE_PSR_TYPES", None), getattr(_EXT, "RENEWABLE_PSR_TYPES", None))

PricesBronzeTask = _prefer(getattr(_Y2030, "PricesBronzeTask", None), getattr(_EXT, "PricesBronzeTask", None))
GenerationBronzeTask = _prefer(getattr(_Y2030, "GenerationBronzeTask", None), getattr(_EXT, "GenerationBronzeTask", None))
LoadBronzeTask = _prefer(getattr(_Y2030, "LoadBronzeTask", None), getattr(_EXT, "LoadBronzeTask", None))
FlowsBronzeTask = _prefer(getattr(_Y2030, "FlowsBronzeTask", None), getattr(_EXT, "FlowsBronzeTask", None))

SilverPricesTask = _prefer(getattr(_Y2030, "SilverPricesTask", None), getattr(_EXT, "SilverPricesTask", None))
SilverGenerationTask = _prefer(getattr(_Y2030, "SilverGenerationTask", None), getattr(_EXT, "SilverGenerationTask", None))
SilverLoadTask = _prefer(getattr(_Y2030, "SilverLoadTask", None), getattr(_EXT, "SilverLoadTask", None))
SilverFlowsTask = _prefer(getattr(_Y2030, "SilverFlowsTask", None), getattr(_EXT, "SilverFlowsTask", None))

RegimeModel = getattr(_EXT, "RegimeModel", None)
RegimeDetector = _prefer(getattr(_Y2030, "RegimeDetector", None), getattr(_CONS, "RegimeDetector", None), getattr(_EXT, "RegimeDetector", None))

# Legacy Gold task names still come from the extension layer.
FactPowerPricesTask = getattr(_EXT, "FactPowerPricesTask", None)
MartDailyMarketTask = getattr(_EXT, "MartDailyMarketTask", None)
MartPriceSpreadsTask = getattr(_EXT, "MartPriceSpreadsTask", None)
MartRegimeSignalsTask = getattr(_EXT, "MartRegimeSignalsTask", None)

# Canonical richer Gold/Platinum tasks come from 2030.
GoldRenewableStabilityTask = getattr(_Y2030, "GoldRenewableStabilityTask", None)
GoldPriceSpikeTask = getattr(_Y2030, "GoldPriceSpikeTask", None)
GoldMarketSummaryTask = getattr(_Y2030, "GoldMarketSummaryTask", None)
GoldImportDependencyTask = getattr(_Y2030, "GoldImportDependencyTask", None)
GoldRegimeSignalsTask = getattr(_Y2030, "GoldRegimeSignalsTask", None)
PlatinumCarbonAdjustedPricesTask = getattr(_Y2030, "PlatinumCarbonAdjustedPricesTask", None)
PlatinumArbitrageOptimizerTask = getattr(_Y2030, "PlatinumArbitrageOptimizerTask", None)

AuditLogTask = _prefer(getattr(_Y2030, "AuditLogTask", None), getattr(_EXT, "AuditLogTask", None))
DQCriticalFailure = _prefer(getattr(_Y2030, "DQCriticalFailure", None), getattr(_EXT, "DQCriticalFailure", None))
DQValidator = getattr(_EXT, "DQValidator", None)
DoraIncidentClassifier = getattr(_EXT, "DoraIncidentClassifier", None)
GdprErasurePipeline = getattr(_EXT, "GdprErasurePipeline", None)
PiiTagger = getattr(_EXT, "PiiTagger", None)
PipelineRunner = _prefer(getattr(_Y2030, "PipelineRunner", None), getattr(_EXT, "PipelineRunner", None))
generate_production_scaffold = getattr(_EXT, "generate_production_scaffold", None)
main = getattr(_EXT, "main", None)

# Consolidated and 2030 advanced surfaces prefer the 2030 core and fall back to
# the consolidated layer when a symbol was not carried forward.
Layer = _prefer(getattr(_Y2030, "Layer", None), getattr(_CONS, "Layer", None))
Status = _prefer(getattr(_Y2030, "Status", None), getattr(_CONS, "Status", None))
Severity = _prefer(getattr(_Y2030, "Severity", None), getattr(_CONS, "Severity", None))
Dataset = _prefer(getattr(_Y2030, "Dataset", None), getattr(_CONS, "Dataset", None))
WriteStrategy = _prefer(getattr(_Y2030, "WriteStrategy", None), getattr(_CONS, "WriteStrategy", None))
RunContext = _prefer(getattr(_Y2030, "RunContext", None), getattr(_CONS, "RunContext", None))
TaskResult = _prefer(getattr(_Y2030, "TaskResult", None), getattr(_CONS, "TaskResult", None))
FieldContract = _prefer(getattr(_Y2030, "FieldContract", None), getattr(_CONS, "FieldContract", None))
TableContract = _prefer(getattr(_Y2030, "TableContract", None), getattr(_CONS, "TableContract", None))
Rule = _prefer(getattr(_Y2030, "Rule", None), getattr(_CONS, "Rule", None))
RuleResult = _prefer(getattr(_Y2030, "RuleResult", None), getattr(_CONS, "RuleResult", None))
BackfillWindow = _prefer(getattr(_Y2030, "BackfillWindow", None), getattr(_CONS, "BackfillWindow", None))
CONTRACTS = _prefer(getattr(_Y2030, "CONTRACTS", None), getattr(_CONS, "CONTRACTS", None))
RULES = _prefer(getattr(_Y2030, "RULES", None), getattr(_CONS, "RULES", None))
ContractError = _prefer(getattr(_Y2030, "ContractError", None), getattr(_CONS, "ContractError", None))
ContractValidator = _prefer(
    getattr(_Y2030, "ContractValidator", None),
    getattr(_CONS, "ContractValidator", None),
)
QualityEngine = _prefer(getattr(_Y2030, "QualityEngine", None), getattr(_CONS, "QualityEngine", None))
ManifestStore = _prefer(getattr(_Y2030, "ManifestStore", None), getattr(_CONS, "ManifestStore", None))
CheckpointStore = _prefer(getattr(_Y2030, "CheckpointStore", None), getattr(_CONS, "CheckpointStore", None))
RetryableError = _prefer(getattr(_Y2030, "RetryableError", None), getattr(_CONS, "RetryableError", None))
RetryPolicy = _prefer(getattr(_Y2030, "RetryPolicy", None), getattr(_CONS, "RetryPolicy", None))
BackfillPlanner = _prefer(getattr(_Y2030, "BackfillPlanner", None), getattr(_CONS, "BackfillPlanner", None))
FeatureBuilder = _prefer(getattr(_Y2030, "FeatureBuilder", None), getattr(_CONS, "FeatureBuilder", None))
GoldBuilder = _prefer(getattr(_Y2030, "GoldBuilder", None), getattr(_CONS, "GoldBuilder", None))
TrainedModel = _prefer(getattr(_Y2030, "TrainedModel", None), getattr(_CONS, "TrainedModel", None))
AnomalyScorer = _prefer(getattr(_Y2030, "AnomalyScorer", None), getattr(_CONS, "AnomalyScorer", None))
PriceForecaster = _prefer(getattr(_Y2030, "PriceForecaster", None), getattr(_CONS, "PriceForecaster", None))
ObservabilityReporter = _prefer(
    getattr(_Y2030, "ObservabilityReporter", None),
    getattr(_CONS, "ObservabilityReporter", None),
)
generate_scaffold = _prefer(getattr(_Y2030, "generate_scaffold", None), getattr(_CONS, "generate_scaffold", None))
describe_architecture = _prefer(
    getattr(_Y2030, "describe_architecture", None),
    getattr(_CONS, "describe_architecture", None),
)
ARCHITECTURE_2030_PRINCIPLES = getattr(_Y2030, "ARCHITECTURE_2030_PRINCIPLES", None)
describe_2030_architecture = getattr(_Y2030, "describe_2030_architecture", None)
LAKEFLOW_GENERATION_PIPELINE_TEMPLATE = getattr(_Y2030, "LAKEFLOW_GENERATION_PIPELINE_TEMPLATE", None)
LAKEFLOW_MARKET_STRESS_TEMPLATE = getattr(_Y2030, "LAKEFLOW_MARKET_STRESS_TEMPLATE", None)
write_lakeflow_templates = getattr(_Y2030, "write_lakeflow_templates", None)
ExecutionMode = getattr(_Y2030, "ExecutionMode", None)
HybridExecutionPlan = getattr(_Y2030, "HybridExecutionPlan", None)
HybridExecutionPlanner = getattr(_Y2030, "HybridExecutionPlanner", None)
MarketStressRecord = getattr(_Y2030, "MarketStressRecord", None)
MarketStressScorer = getattr(_Y2030, "MarketStressScorer", None)
build_market_stress_from_local_records = getattr(_Y2030, "build_market_stress_from_local_records", None)
ForecastRecord = getattr(_Y2030, "ForecastRecord", None)
SimpleRobustForecaster = getattr(_Y2030, "SimpleRobustForecaster", None)
ForecastRiskClassifier = getattr(_Y2030, "ForecastRiskClassifier", None)
EnergyScenario = getattr(_Y2030, "EnergyScenario", None)
ScenarioResult = getattr(_Y2030, "ScenarioResult", None)
ScenarioSimulator = getattr(_Y2030, "ScenarioSimulator", None)
generate_default_scenarios = getattr(_Y2030, "generate_default_scenarios", None)
Recommendation = getattr(_Y2030, "Recommendation", None)
EnergyRecommendationEngine = getattr(_Y2030, "EnergyRecommendationEngine", None)
ALLOWED_AGENT_TABLES = getattr(_Y2030, "ALLOWED_AGENT_TABLES", None)
AgentQueryPlan = getattr(_Y2030, "AgentQueryPlan", None)
GovernedEnergyAgentPlanner = getattr(_Y2030, "GovernedEnergyAgentPlanner", None)
FindingsReportBuilder = getattr(_Y2030, "FindingsReportBuilder", None)
Local2030IntelligenceRunner = getattr(_Y2030, "Local2030IntelligenceRunner", None)
run_2030_command = getattr(_Y2030, "run_2030_command", None)
