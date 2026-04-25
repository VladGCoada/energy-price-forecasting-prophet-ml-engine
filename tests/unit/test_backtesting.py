from eu_energy_intelligence.intelligence.backtesting import compute_backtest_metrics


def test_compute_backtest_metrics_returns_expected_shape() -> None:
    metrics = compute_backtest_metrics(
        actual=[10.0, 20.0, 30.0],
        predicted=[12.0, 18.0, 33.0],
    )
    assert metrics.actual_count == 3
    assert metrics.mae > 0
    assert metrics.rmse > 0
    assert metrics.mape_pct > 0
    assert metrics.smape_pct > 0
