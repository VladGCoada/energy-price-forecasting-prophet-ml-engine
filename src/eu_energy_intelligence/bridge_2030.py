from __future__ import annotations

from importlib import import_module
from types import ModuleType


def _load_2030() -> ModuleType:
    """Return the package-native 2030 intelligence module."""
    return import_module("eu_energy_intelligence.production_2030")


_M2030 = _load_2030()

ARCHITECTURE_2030_PRINCIPLES = _M2030.ARCHITECTURE_2030_PRINCIPLES
describe_2030_architecture = _M2030.describe_2030_architecture
LAKEFLOW_GENERATION_PIPELINE_TEMPLATE = _M2030.LAKEFLOW_GENERATION_PIPELINE_TEMPLATE
LAKEFLOW_MARKET_STRESS_TEMPLATE = _M2030.LAKEFLOW_MARKET_STRESS_TEMPLATE
write_lakeflow_templates = _M2030.write_lakeflow_templates
ExecutionMode = _M2030.ExecutionMode
HybridExecutionPlan = _M2030.HybridExecutionPlan
HybridExecutionPlanner = _M2030.HybridExecutionPlanner
MarketStressRecord = _M2030.MarketStressRecord
MarketStressScorer = _M2030.MarketStressScorer
build_market_stress_from_local_records = _M2030.build_market_stress_from_local_records
ForecastRecord = _M2030.ForecastRecord
SimpleRobustForecaster = _M2030.SimpleRobustForecaster
ForecastRiskClassifier = _M2030.ForecastRiskClassifier
EnergyScenario = _M2030.EnergyScenario
ScenarioResult = _M2030.ScenarioResult
ScenarioSimulator = _M2030.ScenarioSimulator
generate_default_scenarios = _M2030.generate_default_scenarios
Recommendation = _M2030.Recommendation
EnergyRecommendationEngine = _M2030.EnergyRecommendationEngine
ALLOWED_AGENT_TABLES = _M2030.ALLOWED_AGENT_TABLES
AgentQueryPlan = _M2030.AgentQueryPlan
GovernedEnergyAgentPlanner = _M2030.GovernedEnergyAgentPlanner
FindingsReportBuilder = _M2030.FindingsReportBuilder
Local2030IntelligenceRunner = _M2030.Local2030IntelligenceRunner
run_2030_command = _M2030.run_2030_command
