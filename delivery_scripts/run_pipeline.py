"""
Project-level runner for the BDA P2 pipeline.

It orchestrates the data zones, the Knowledge Graph exploitation zone, the
SPARQL analysis pipeline and the KGE factor recommendation pipeline.
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent
PYTHON = Path(sys.executable)

STAGE_COMMANDS = {
    "landing": ["scripts/data_collector.py"],
    "formatted": ["scripts/formatted_zone.py"],
    "trusted": ["scripts/trusted_zone.py"],
    "exploitation": ["scripts/exploitation_zone.py"],
    "sparql": [
        "scripts/sparql_analysis.py",
        "--query",
        "queries",
        "--output-dir",
        "outputs/sparql",
    ],
    "kge": ["scripts/kge_factor_recommendation.py"],
}

DEFAULT_FULL_PIPELINE = [
    "landing",
    "formatted",
    "trusted",
    "exploitation",
    "sparql",
    "kge",
]


def run_command(command: list[str], dry_run: bool = False) -> None:
    printable = " ".join(str(part) for part in [PYTHON, *command])
    print(f"\n$ {printable}")

    if dry_run:
        return

    subprocess.check_call([str(PYTHON), *command], cwd=PROJECT_ROOT)


def install_requirements(dry_run: bool = False) -> None:
    run_command(["-m", "pip", "install", "-r", "requirements.txt"], dry_run=dry_run)


def resolve_stages(requested_stages: list[str], skip_landing: bool, skip_kge: bool) -> list[str]:
    if "all" in requested_stages:
        stages = list(DEFAULT_FULL_PIPELINE)
    else:
        stages = requested_stages

    if skip_landing:
        stages = [stage for stage in stages if stage != "landing"]
    if skip_kge:
        stages = [stage for stage in stages if stage != "kge"]

    return stages


def build_stage_command(stage: str, args: argparse.Namespace) -> list[str]:
    command = list(STAGE_COMMANDS[stage])

    if stage == "landing" and args.date:
        command.extend(["--date", args.date])

    if stage == "kge":
        if args.quick_kge:
            command.append("--quick")
        if args.recommendation_days is not None:
            command.extend(["--recommendation-days", str(args.recommendation_days)])
        if args.top_k is not None:
            command.extend(["--top-k", str(args.top_k)])

    return command


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the BDA P2 data, KG, SPARQL and KGE pipelines.")
    parser.add_argument(
        "stages",
        nargs="*",
        default=["all"],
        choices=[*STAGE_COMMANDS.keys(), "all"],
        help="Pipeline stages to run. Defaults to all.",
    )
    parser.add_argument(
        "--install-requirements",
        action="store_true",
        help="Install requirements before running the selected stages.",
    )
    parser.add_argument(
        "--date",
        help="Execution date for the landing zone in YYYY-MM-DD format.",
    )
    parser.add_argument(
        "--skip-landing",
        action="store_true",
        help="Skip Kaggle download when running all stages.",
    )
    parser.add_argument(
        "--skip-kge",
        action="store_true",
        help="Skip KGE training when running all stages.",
    )
    parser.add_argument(
        "--quick-kge",
        action="store_true",
        help="Use the fast KGE configuration for smoke tests.",
    )
    parser.add_argument(
        "--recommendation-days",
        type=int,
        help="Number of days used for KGE recommendation examples.",
    )
    parser.add_argument(
        "--top-k",
        type=int,
        help="Number of KGE factor recommendations per day.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print commands without executing them.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    stages = resolve_stages(args.stages, args.skip_landing, args.skip_kge)

    if args.install_requirements:
        print("Installing requirements...")
        install_requirements(dry_run=args.dry_run)

    print("Executing BDA P2 pipeline")
    print(f"Stages: {', '.join(stages)}")

    for position, stage in enumerate(stages, start=1):
        print(f"\n{position}. {stage.upper()} stage")
        run_command(build_stage_command(stage, args), dry_run=args.dry_run)

    print("\nPipeline completed successfully.")


if __name__ == "__main__":
    main()
