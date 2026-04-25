from eu_energy_intelligence.intelligence import (
    GovernedEnergyAgentPlanner,
    MarketStressScorer,
    ScenarioSimulator,
    SimpleRobustForecaster,
)
from eu_energy_intelligence.orchestration import (
    ExecutionMode,
    HybridExecutionPlanner,
    describe_2030_architecture,
)


def test_2030_architecture_metadata_is_available() -> None:
    arch = describe_2030_architecture()
    assert arch["name"] == "EU Energy Intelligence Platform 2030"
    assert "platinum" in arch["layers"]


def test_hybrid_execution_planner_produces_streaming_plan() -> None:
    plan = HybridExecutionPlanner().plan_streaming_refresh(["NL"])
    assert plan.mode == ExecutionMode.STREAMING
    assert "generation" in plan.datasets


def test_market_stress_scorer_detects_main_driver() -> None:
    row = {
        "country_code": "NL",
        "event_date": "2024-01-01",
        "avg_price_eur_mwh": 100.0,
        "max_price_eur_mwh": 180.0,
        "price_volatility": 20.0,
        "price_spike_count": 0,
        "negative_price_count": 0,
        "renewable_share_pct": 10.0,
        "renewable_volatility_index": 0.1,
        "avg_load_mw": 12000.0,
        "avg_forecast_error_mw": 100.0,
    }
    result = MarketStressScorer().score_row(row)
    assert result.main_driver == "LOW_RENEWABLE_SHARE"


def test_scenario_simulator_increases_stress_under_shock() -> None:
    baseline = {
        "country_code": "NL",
        "event_date": "2024-01-01",
        "avg_price_eur_mwh": 50.0,
        "max_price_eur_mwh": 100.0,
        "price_volatility": 10.0,
        "price_spike_count": 0,
        "negative_price_count": 0,
        "renewable_share_pct": 50.0,
        "renewable_volatility_index": 0.1,
        "avg_load_mw": 10000.0,
        "avg_forecast_error_mw": 100.0,
        "stress_score": 0.2,
    }
    from eu_energy_intelligence.intelligence import EnergyScenario
    import uuid

    scenario = EnergyScenario(
        str(uuid.uuid4()),
        "combined",
        "NL",
        "2024-01-01",
        wind_drop_pct=30,
        load_increase_pct=20,
        price_shock_eur_mwh=100,
    )
    result = ScenarioSimulator().simulate(baseline, scenario)
    assert "simulated_stress_score" in result
    assert "baseline_stress_score" in result
    assert "recommendation" in result
    assert result["simulated_label"] in {"LOW", "MEDIUM", "HIGH", "CRITICAL"}


def test_agent_planner_uses_approved_tables_only() -> None:
    plan = GovernedEnergyAgentPlanner().plan("Which days had highest stress?")
    assert "gold_market_stress_daily" in plan.allowed_tables[0]
    assert "bronze" not in plan.sql.lower()


def test_simple_robust_forecaster_outputs_requested_horizon() -> None:
    timestamps = [f"2024-01-01T{i:02d}:00:00+00:00" for i in range(24)]
    values = [50.0 + i for i in range(24)]
    forecasts = SimpleRobustForecaster(horizon_intervals=4, season_lag=4, rolling_window=8).forecast_series(
        "NL",
        timestamps,
        values,
    )
    assert len(forecasts) == 4
    assert forecasts[0]["zone"] == "NL"
