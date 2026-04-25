"""Intelligence and regime-detection helpers."""

try:
    from eu_energy_intelligence.intelligence.anomaly import AnomalyScorer
except ImportError:  # pragma: no cover - optional ML dependency
    AnomalyScorer = None  # type: ignore[assignment]
from eu_energy_intelligence.intelligence.backtesting import (
    BacktestMetrics,
    compute_backtest_metrics,
    run_prophet_price_backtest,
)
from eu_energy_intelligence.intelligence.agent import (
    ALLOWED_AGENT_TABLES,
    AgentQueryPlan,
    GovernedEnergyAgentPlanner,
)
from eu_energy_intelligence.intelligence.forecaster import PriceForecaster, TrainedModel
from eu_energy_intelligence.intelligence.forecasting import RollingWindowForecaster
from eu_energy_intelligence.intelligence.input_diagnostics import summarize_processed_inputs
from eu_energy_intelligence.intelligence.local_ml import (
    LocalAnomalyModel,
    build_local_feature_rows,
    run_local_ml_pipeline,
    run_local_price_forecast,
)
from eu_energy_intelligence.intelligence.market_stress import (
    MarketStressRecord,
    MarketStressScorer,
    build_market_stress_from_local_records,
)
from eu_energy_intelligence.intelligence.recommendations import (
    EnergyRecommendationEngine,
    Recommendation,
)
from eu_energy_intelligence.intelligence.reports import FindingsReportBuilder
from eu_energy_intelligence.intelligence.regime import RegimeDetector, RegimeModel
from eu_energy_intelligence.intelligence.robust_forecasting import (
    ForecastRecord,
    ForecastRiskClassifier,
    SimpleRobustForecaster,
)
from eu_energy_intelligence.intelligence.scenarios import (
    EnergyScenario,
    ScenarioResult,
    ScenarioSimulator,
    generate_default_scenarios,
)

__all__ = [
    "ALLOWED_AGENT_TABLES",
    "AnomalyScorer",
    "AgentQueryPlan",
    "BacktestMetrics",
    "EnergyRecommendationEngine",
    "EnergyScenario",
    "FindingsReportBuilder",
    "ForecastRecord",
    "ForecastRiskClassifier",
    "GovernedEnergyAgentPlanner",
    "LocalAnomalyModel",
    "MarketStressRecord",
    "MarketStressScorer",
    "PriceForecaster",
    "Recommendation",
    "RegimeDetector",
    "RegimeModel",
    "RollingWindowForecaster",
    "ScenarioResult",
    "ScenarioSimulator",
    "SimpleRobustForecaster",
    "TrainedModel",
    "build_local_feature_rows",
    "build_market_stress_from_local_records",
    "compute_backtest_metrics",
    "generate_default_scenarios",
    "run_local_ml_pipeline",
    "run_local_price_forecast",
    "run_prophet_price_backtest",
    "summarize_processed_inputs",
]
