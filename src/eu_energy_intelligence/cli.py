from __future__ import annotations

import argparse
from datetime import UTC, datetime
import json
from typing import Sequence

from eu_energy_intelligence.backfill import BackfillPlanner
from eu_energy_intelligence.bridge_2030 import describe_2030_architecture, run_2030_command, write_lakeflow_templates
from eu_energy_intelligence.compliance import GdprErasurePipeline
from eu_energy_intelligence.bronze.tasks import (
    FlowsBronzeTask,
    GenerationBronzeTask,
    LoadBronzeTask,
    PricesBronzeTask,
)
from eu_energy_intelligence.consolidated_bridge import describe_architecture, generate_scaffold
from eu_energy_intelligence.gold import build_renewable_stability
from eu_energy_intelligence.gold.tasks import (
    FactPowerPricesTask,
    MartDailyMarketTask,
    MartPriceSpreadsTask,
    MartRegimeSignalsTask,
)
from eu_energy_intelligence.ingestion import probe_entsoe_overlap
from eu_energy_intelligence.ingestion.weather_client import WeatherClient
from eu_energy_intelligence.intelligence.backtesting import run_prophet_price_backtest
from eu_energy_intelligence.intelligence.input_diagnostics import summarize_processed_inputs
from eu_energy_intelligence.intelligence.local_ml import run_local_ml_pipeline
from eu_energy_intelligence.orchestration.local import run_local_generation_pipeline
from eu_energy_intelligence.orchestration.production import ProductionPipelineRunner
from eu_energy_intelligence.platinum.tasks import (
    PlatinumArbitrageOptimizerTask,
    PlatinumCarbonAdjustedPricesTask,
)
from eu_energy_intelligence.reliability import CheckpointStore
from eu_energy_intelligence.scaffold import generate_production_scaffold
from eu_energy_intelligence.settings import PlatformConfig
from eu_energy_intelligence.silver.tasks import (
    SilverFlowsTask,
    SilverGenerationTask,
    SilverLoadTask,
    SilverPricesTask,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="eu-energy-intelligence")
    subparsers = parser.add_subparsers(dest="command", required=True)

    run_local = subparsers.add_parser("run-local-generation")
    run_local.add_argument("--raw-file", required=True)
    run_local.add_argument("--country-code", default="DE")
    run_local.add_argument("--processed-base-dir", default="data/processed")

    gold_demo = subparsers.add_parser("gold-demo")
    gold_demo.add_argument("--country-code", default="DE")
    gold_demo.add_argument("--quantities", nargs="+", type=float, required=True)

    weather_demo = subparsers.add_parser("weather-params")
    weather_demo.add_argument("--latitude", type=float, required=True)
    weather_demo.add_argument("--longitude", type=float, required=True)
    weather_demo.add_argument("--start-date", required=True)
    weather_demo.add_argument("--end-date", required=True)

    subparsers.add_parser("scaffold-prod")
    subparsers.add_parser("run-pipeline")
    subparsers.add_parser("run-bronze")
    subparsers.add_parser("run-silver")
    subparsers.add_parser("run-gold")
    subparsers.add_parser("run-platinum")
    subparsers.add_parser("run-erasure")
    subparsers.add_parser("describe-architecture")
    subparsers.add_parser("scaffold-consolidated")
    subparsers.add_parser("describe-2030")
    subparsers.add_parser("write-lakeflow")
    local_2030 = subparsers.add_parser("run-local-intelligence")
    local_2030.add_argument("--processed-base-dir", default="data/processed")
    agent_plan = subparsers.add_parser("agent-plan")
    agent_plan.add_argument("question")
    check_backfill = subparsers.add_parser("check-backfill")
    check_backfill.add_argument("--target-date", default=datetime.now(UTC).date().isoformat())
    check_backfill.add_argument("--fallback-start", default="2020-01-01")
    overlap_probe = subparsers.add_parser("probe-entsoe-overlap")
    overlap_probe.add_argument("--zone", required=True)
    overlap_probe.add_argument("--flow-partner", default=None)
    overlap_probe.add_argument("--start-date", required=True)
    overlap_probe.add_argument("--end-date", required=True)
    input_diag = subparsers.add_parser("inspect-intelligence-inputs")
    input_diag.add_argument("--processed-base-dir", default="data/processed")
    local_ml = subparsers.add_parser("run-local-ml")
    local_ml.add_argument("--processed-base-dir", default="data/processed")
    local_ml.add_argument("--zone", default="NL")
    local_ml.add_argument("--horizon-intervals", type=int, default=96)
    backtest = subparsers.add_parser("backtest-price-forecast")
    backtest.add_argument("--zone", default="NL")
    backtest.add_argument("--train-start", required=True)
    backtest.add_argument("--train-end", required=True)
    backtest.add_argument("--test-start", required=True)
    backtest.add_argument("--test-end", required=True)
    backtest.add_argument("--processed-base-dir", default="data/processed")

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command == "run-local-generation":
        result = run_local_generation_pipeline(
            raw_file=args.raw_file,
            country_code=args.country_code,
            processed_base_dir=args.processed_base_dir,
        )
        print(json.dumps(result, indent=2))
        return 0

    if args.command == "gold-demo":
        rows = [{"country_code": args.country_code, "quantity": quantity} for quantity in args.quantities]
        print(json.dumps(build_renewable_stability(rows), indent=2))
        return 0

    if args.command == "weather-params":
        client = WeatherClient()
        print(
            json.dumps(
                client.build_params(
                    latitude=args.latitude,
                    longitude=args.longitude,
                    start_date=args.start_date,
                    end_date=args.end_date,
                ),
                indent=2,
            )
        )
        return 0

    if args.command == "scaffold-prod":
        generate_production_scaffold(".")
        return 0

    if args.command == "run-pipeline":
        print(json.dumps(ProductionPipelineRunner(PlatformConfig()).run(), indent=2, default=str))
        return 0

    if args.command == "run-bronze":
        config = PlatformConfig()
        for task_class in [PricesBronzeTask, GenerationBronzeTask, LoadBronzeTask, FlowsBronzeTask]:
            print(f"{task_class.__name__}: {task_class(config).run()}")
        return 0

    if args.command == "run-silver":
        config = PlatformConfig()
        for task_class in [SilverPricesTask, SilverGenerationTask, SilverLoadTask, SilverFlowsTask]:
            print(f"{task_class.__name__}: {task_class(config).run()}")
        return 0

    if args.command == "run-gold":
        config = PlatformConfig()
        for task_class in [
            FactPowerPricesTask,
            MartDailyMarketTask,
            MartPriceSpreadsTask,
            MartRegimeSignalsTask,
        ]:
            print(f"{task_class.__name__}: {task_class(config).run()}")
        return 0

    if args.command == "run-platinum":
        config = PlatformConfig()
        for task_class in [PlatinumCarbonAdjustedPricesTask, PlatinumArbitrageOptimizerTask]:
            print(f"{task_class.__name__}: {task_class(config).run()}")
        return 0

    if args.command == "run-erasure":
        print(
            json.dumps(
                GdprErasurePipeline(PlatformConfig()).process_pending_requests(),
                indent=2,
                default=str,
            )
        )
        return 0

    if args.command == "describe-architecture":
        print(json.dumps(describe_architecture(), indent=2, default=str))
        return 0

    if args.command == "scaffold-consolidated":
        generate_scaffold(".")
        return 0

    if args.command == "check-backfill":
        config = PlatformConfig()
        planner = BackfillPlanner(CheckpointStore(config.checkpoint_dir))
        print(
            json.dumps(
                planner.summary(
                    datasets=["prices", "generation", "load", "flows"],
                    entities=config.bidding_zones,
                    target_date=args.target_date,
                    fallback_start=args.fallback_start,
                ),
                indent=2,
                default=str,
            )
        )
        return 0

    if args.command == "probe-entsoe-overlap":
        print(
            json.dumps(
                probe_entsoe_overlap(
                    zone=args.zone,
                    flow_partner=args.flow_partner,
                    start_date=args.start_date,
                    end_date=args.end_date,
                ),
                indent=2,
                default=str,
            )
        )
        return 0

    if args.command == "inspect-intelligence-inputs":
        print(
            json.dumps(
                summarize_processed_inputs(args.processed_base_dir),
                indent=2,
                default=str,
            )
        )
        return 0

    if args.command == "run-local-ml":
        print(
            json.dumps(
                run_local_ml_pipeline(
                    processed_base_dir=args.processed_base_dir,
                    zone=args.zone.upper(),
                    horizon_intervals=args.horizon_intervals,
                ),
                indent=2,
                default=str,
            )
        )
        return 0

    if args.command == "backtest-price-forecast":
        print(
            json.dumps(
                run_prophet_price_backtest(
                    zone=args.zone.upper(),
                    train_start=args.train_start,
                    train_end=args.train_end,
                    test_start=args.test_start,
                    test_end=args.test_end,
                    processed_base_dir=args.processed_base_dir,
                ),
                indent=2,
                default=str,
            )
        )
        return 0

    if args.command == "describe-2030":
        print(json.dumps(describe_2030_architecture(), indent=2, default=str))
        return 0

    if args.command == "write-lakeflow":
        print(json.dumps({"written": write_lakeflow_templates()}, indent=2, default=str))
        return 0

    if args.command == "run-local-intelligence":
        return run_2030_command(
            ["run-local-intelligence", "--processed-base-dir", args.processed_base_dir]
        )

    if args.command == "agent-plan":
        return run_2030_command(["agent-plan", args.question])

    parser.error(f"Unknown command: {args.command}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
