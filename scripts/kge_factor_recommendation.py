"""
Knowledge Graph Embeddings pipeline for contributing-factor recommendation.

The task is link prediction over a derived relation:

    (day/YYYY-MM-DD, hasLikelyContributingFactor, factor/<factor-slug>)

The relation is created from the existing KG path Day -> FactorCount -> ContributingFactor,
but those original factor-count edges are deliberately excluded from training context to
avoid leaking the target links into the embeddings.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import random
from dataclasses import dataclass
from pathlib import Path
from typing import Any

os.environ.setdefault("PYSTOW_HOME", str(Path(__file__).resolve().parents[1] / ".pystow"))

import duckdb
import numpy as np
import pandas as pd
import torch
from pykeen.pipeline import pipeline
from pykeen.predict import predict_target
from pykeen.triples import TriplesFactory


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DB_PATH = PROJECT_ROOT / "exploitation_zone.db"
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "outputs" / "kge"
TARGET_RELATION = "hasLikelyContributingFactor"
RANDOM_SEED = 42

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class KGEConfig:
    model_name: str
    embedding_dim: int
    learning_rate: float
    epochs: int
    negative_samples: int

    @property
    def run_id(self) -> str:
        return (
            f"{self.model_name}_dim{self.embedding_dim}_"
            f"lr{self.learning_rate:g}_ep{self.epochs}_neg{self.negative_samples}"
        )


def slugify_series(series: pd.Series) -> pd.Series:
    return (
        series.astype(str)
        .str.normalize("NFKD")
        .str.encode("ascii", errors="ignore")
        .str.decode("ascii")
        .str.lower()
        .str.replace(r"[^a-z0-9]+", "-", regex=True)
        .str.strip("-")
        .replace("", "unknown")
    )


def load_source_tables(db_path: Path) -> dict[str, pd.DataFrame]:
    if not db_path.is_file():
        raise FileNotFoundError(f"Exploitation database not found: {db_path}")

    with duckdb.connect(str(db_path), read_only=True) as conn:
        tables = {
            "nodes": conn.execute("SELECT node_id, node_type, label FROM kg_nodes").fetchdf(),
            "edges": conn.execute(
                """
                SELECT source_id, predicate, target_id
                FROM kg_edges
                WHERE predicate NOT IN ('hasFactorCount', 'aboutFactor')
                """
            ).fetchdf(),
            "factor_daily": conn.execute(
                """
                SELECT CAST(event_date AS VARCHAR) AS event_date, factor, occurrences
                FROM factor_daily
                WHERE factor IS NOT NULL
                """
            ).fetchdf(),
            "weather": conn.execute(
                """
                SELECT
                    CAST(event_date AS VARCHAR) AS event_date,
                    weather_avg_temperature,
                    weather_precipitation,
                    weather_snow_fall,
                    weather_snow_depth
                FROM weather_daily
                """
            ).fetchdf(),
            "mobility": conn.execute(
                """
                SELECT
                    CAST(event_date AS VARCHAR) AS event_date,
                    uber_total_dispatched_trips
                FROM uber_daily
                """
            ).fetchdf(),
            "safety": conn.execute(
                """
                SELECT
                    CAST(event_date AS VARCHAR) AS event_date,
                    borough,
                    collisions
                FROM borough_daily_safety
                WHERE borough IS NOT NULL
                """
            ).fetchdf(),
            "day_literals": conn.execute(
                """
                SELECT
                    REPLACE(subject_id, 'day/', '') AS event_date,
                    value AS is_weekend
                FROM kg_literals
                WHERE predicate = 'isWeekend'
                """
            ).fetchdf(),
        }
    return tables


def make_target_triples(factor_daily: pd.DataFrame, min_occurrences: int) -> pd.DataFrame:
    targets = factor_daily[factor_daily["occurrences"] >= min_occurrences].copy()
    targets["head"] = "day/" + targets["event_date"].astype(str)
    targets["relation"] = TARGET_RELATION
    targets["tail"] = "factor/" + slugify_series(targets["factor"])
    targets["factor_label"] = targets["factor"]
    return targets[["head", "relation", "tail", "event_date", "factor_label", "occurrences"]]


def make_context_triples(tables: dict[str, pd.DataFrame]) -> pd.DataFrame:
    triples: list[tuple[str, str, str]] = []

    for row in tables["edges"].itertuples(index=False):
        triples.append((row.source_id, row.predicate, row.target_id))

    day_literals = tables["day_literals"].copy()
    for row in day_literals.itertuples(index=False):
        period = "weekend" if str(row.is_weekend).lower() == "true" else "weekday"
        triples.append((f"day/{row.event_date}", "hasDayType", f"day-type/{period}"))

    weather = tables["weather"].copy()
    weather["temperature_level"] = pd.cut(
        weather["weather_avg_temperature"],
        bins=[-np.inf, 40.0, 70.0, np.inf],
        labels=["cold", "mild", "hot"],
    )
    weather["precipitation_level"] = pd.cut(
        weather["weather_precipitation"],
        bins=[-np.inf, 0.0, 0.2, np.inf],
        labels=["none", "low", "high"],
    )
    for row in weather.itertuples(index=False):
        day = f"day/{row.event_date}"
        triples.append((day, "hasTemperatureLevel", f"temperature-level/{row.temperature_level}"))
        triples.append((day, "hasPrecipitationLevel", f"precipitation-level/{row.precipitation_level}"))
        if float(row.weather_snow_fall or 0.0) > 0.0 or float(row.weather_snow_depth or 0.0) > 0.0:
            triples.append((day, "hasSnowCondition", "snow-condition/snow"))
        else:
            triples.append((day, "hasSnowCondition", "snow-condition/no-snow"))

    mobility = tables["mobility"].copy()
    mobility["mobility_level"] = pd.qcut(
        mobility["uber_total_dispatched_trips"],
        q=3,
        labels=["low", "medium", "high"],
        duplicates="drop",
    )
    for row in mobility.itertuples(index=False):
        triples.append((f"day/{row.event_date}", "hasMobilityLevel", f"mobility-level/{row.mobility_level}"))

    safety = tables["safety"].copy()
    safety["borough_slug"] = slugify_series(safety["borough"])
    safety["collision_level"] = safety.groupby("borough")["collisions"].transform(
        lambda values: pd.qcut(values.rank(method="first"), q=3, labels=["low", "medium", "high"])
    )
    for row in safety.itertuples(index=False):
        day = f"day/{row.event_date}"
        triples.append((day, "hasBorough", f"borough/{row.borough_slug}"))
        triples.append((day, "hasBoroughCollisionLevel", f"borough-collision-level/{row.borough_slug}/{row.collision_level}"))

    context = pd.DataFrame(triples, columns=["head", "relation", "tail"]).drop_duplicates()
    return context


def split_target_triples(
    target_triples: pd.DataFrame,
    test_fraction: float,
    validation_fraction: float,
    seed: int,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    rng = random.Random(seed)
    shuffled_indices = list(target_triples.index)
    rng.shuffle(shuffled_indices)

    train_indices: set[int] = set()
    for _, group in target_triples.groupby("tail"):
        train_indices.add(rng.choice(list(group.index)))
    for _, group in target_triples.groupby("head"):
        train_indices.add(rng.choice(list(group.index)))

    remaining = [idx for idx in shuffled_indices if idx not in train_indices]
    n_total = len(target_triples)
    n_test = max(1, int(n_total * test_fraction))
    n_validation = max(1, int(n_total * validation_fraction))

    test_indices = set(remaining[:n_test])
    validation_indices = set(remaining[n_test : n_test + n_validation])
    train_indices.update(idx for idx in remaining[n_test + n_validation :])

    train = target_triples.loc[sorted(train_indices)].reset_index(drop=True)
    validation = target_triples.loc[sorted(validation_indices)].reset_index(drop=True)
    test = target_triples.loc[sorted(test_indices)].reset_index(drop=True)
    return train, validation, test


def to_labeled_triples(df: pd.DataFrame) -> np.ndarray:
    return df[["head", "relation", "tail"]].astype(str).to_numpy()


def make_triples_factories(
    context_triples: pd.DataFrame,
    train_targets: pd.DataFrame,
    validation_targets: pd.DataFrame,
    test_targets: pd.DataFrame,
) -> tuple[TriplesFactory, TriplesFactory, TriplesFactory]:
    training_df = pd.concat(
        [context_triples, train_targets[["head", "relation", "tail"]]],
        ignore_index=True,
    ).drop_duplicates()

    training = TriplesFactory.from_labeled_triples(to_labeled_triples(training_df))
    validation = TriplesFactory.from_labeled_triples(
        to_labeled_triples(validation_targets),
        entity_to_id=training.entity_to_id,
        relation_to_id=training.relation_to_id,
    )
    testing = TriplesFactory.from_labeled_triples(
        to_labeled_triples(test_targets),
        entity_to_id=training.entity_to_id,
        relation_to_id=training.relation_to_id,
    )
    return training, validation, testing


def build_model_grid(quick: bool) -> list[KGEConfig]:
    epochs = 5 if quick else 25
    negative_samples = 4 if quick else 16
    model_names = ["TransE", "DistMult", "ComplEx", "RotatE"]
    grid = [
        KGEConfig("TransE", 32, 0.01, epochs, negative_samples),
        KGEConfig("TransE", 64, 0.01, epochs, negative_samples),
        KGEConfig("TransE", 64, 0.005, epochs, negative_samples),
    ]
    grid.extend(KGEConfig(model_name, 64, 0.01, epochs, negative_samples) for model_name in model_names[1:])
    return grid


def extract_metric(metric_results: Any, metric_name: str) -> float | None:
    flat = metric_results.to_flat_dict()
    preferred_key = f"both.realistic.{metric_name}"
    if preferred_key in flat:
        return float(flat[preferred_key])

    candidates = [key for key in flat if key.endswith(metric_name) and "realistic" in key and "both" in key]
    if not candidates:
        candidates = [key for key in flat if key.endswith(metric_name) and "realistic" in key]
    if not candidates:
        candidates = [key for key in flat if key.endswith(metric_name)]
    if not candidates:
        return None
    return float(flat[candidates[0]])


def train_and_evaluate(
    config: KGEConfig,
    training: TriplesFactory,
    validation: TriplesFactory,
    testing: TriplesFactory,
    seed: int,
) -> dict[str, Any]:
    LOGGER.info("Training %s", config.run_id)
    result = pipeline(
        training=training,
        validation=validation,
        testing=testing,
        model=config.model_name,
        model_kwargs={"embedding_dim": config.embedding_dim},
        training_loop="sLCWA",
        negative_sampler="basic",
        negative_sampler_kwargs={"num_negs_per_pos": config.negative_samples},
        optimizer="Adam",
        optimizer_kwargs={"lr": config.learning_rate},
        epochs=config.epochs,
        training_kwargs={"batch_size": 512},
        evaluator="RankBasedEvaluator",
        evaluation_kwargs={"batch_size": 512},
        random_seed=seed,
        device="cpu",
        use_tqdm=False,
    )
    metrics = result.metric_results
    return {
        "run_id": config.run_id,
        "model": config.model_name,
        "embedding_dim": config.embedding_dim,
        "learning_rate": config.learning_rate,
        "epochs": config.epochs,
        "negative_samples": config.negative_samples,
        "mrr": extract_metric(metrics, "inverse_harmonic_mean_rank"),
        "hits_at_1": extract_metric(metrics, "hits_at_1"),
        "hits_at_3": extract_metric(metrics, "hits_at_3"),
        "hits_at_10": extract_metric(metrics, "hits_at_10"),
        "pipeline_result": result,
    }


def select_best_model(results: list[dict[str, Any]]) -> dict[str, Any]:
    return max(
        results,
        key=lambda row: (
            row["mrr"] if row["mrr"] is not None else -1.0,
            row["hits_at_10"] if row["hits_at_10"] is not None else -1.0,
        ),
    )


def make_day_context(tables: dict[str, pd.DataFrame]) -> pd.DataFrame:
    weather = tables["weather"]
    mobility = tables["mobility"]
    safety = tables["safety"].groupby("event_date", as_index=False).agg(
        total_collisions=("collisions", "sum"),
        top_borough=("borough", lambda values: values.value_counts().index[0]),
    )
    context = weather.merge(mobility, on="event_date", how="left").merge(safety, on="event_date", how="left")
    return context


def generate_recommendations(
    best_result: Any,
    training: TriplesFactory,
    factor_nodes: list[str],
    factor_labels: dict[str, str],
    train_targets: pd.DataFrame,
    target_triples: pd.DataFrame,
    day_context: pd.DataFrame,
    output_dir: Path,
    n_days: int,
    top_k: int,
) -> pd.DataFrame:
    model = best_result.model
    model.eval()
    candidate_days = sorted(target_triples["head"].unique())[:n_days]
    known_train = set(map(tuple, train_targets[["head", "tail"]].to_numpy()))
    all_known = (
        target_triples.groupby("head")["tail"]
        .apply(lambda values: set(values))
        .to_dict()
    )
    context_by_day = day_context.set_index("event_date").to_dict(orient="index")

    rows: list[dict[str, Any]] = []
    for day in candidate_days:
        predictions = predict_target(
            model=model,
            head=day,
            relation=TARGET_RELATION,
            triples_factory=training,
            targets=factor_nodes,
        ).df
        predictions = predictions.sort_values("score", ascending=False)

        emitted = 0
        for prediction in predictions.itertuples(index=False):
            factor_node = str(prediction.tail_label)
            if (day, factor_node) in known_train:
                continue
            event_date = day.replace("day/", "")
            context = context_by_day.get(event_date, {})
            rows.append(
                {
                    "day": day,
                    "event_date": event_date,
                    "recommended_factor": factor_node,
                    "recommended_factor_label": factor_labels.get(factor_node, factor_node),
                    "score": float(prediction.score),
                    "was_observed_in_full_kg": factor_node in all_known.get(day, set()),
                    "avg_temperature": context.get("weather_avg_temperature"),
                    "precipitation": context.get("weather_precipitation"),
                    "estimated_uber_trips": context.get("uber_total_dispatched_trips"),
                    "total_collisions": context.get("total_collisions"),
                    "top_borough": context.get("top_borough"),
                }
            )
            emitted += 1
            if emitted >= top_k:
                break

    recommendations = pd.DataFrame(rows)
    recommendations.to_csv(output_dir / "factor_recommendations.csv", index=False)
    return recommendations


def write_outputs(
    output_dir: Path,
    results: list[dict[str, Any]],
    best: dict[str, Any],
    target_counts: dict[str, int],
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)

    comparison = pd.DataFrame([{k: v for k, v in row.items() if k != "pipeline_result"} for row in results])
    comparison = comparison.sort_values(["mrr", "hits_at_10"], ascending=False, na_position="last")
    comparison.to_csv(output_dir / "model_comparison.csv", index=False)

    metadata = {
        "task": "Day-to-ContributingFactor link prediction",
        "target_relation": TARGET_RELATION,
        "best_run_id": best["run_id"],
        "best_model": best["model"],
        "split_counts": target_counts,
    }
    (output_dir / "run_metadata.json").write_text(json.dumps(metadata, indent=2), encoding="utf-8")
    best["pipeline_result"].save_to_directory(output_dir / "models" / best["run_id"])


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Train KGE models to recommend contributing factors for KG days.")
    parser.add_argument("--db", type=Path, default=DEFAULT_DB_PATH, help="Path to exploitation_zone.db")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR, help="Directory for KGE outputs")
    parser.add_argument("--min-occurrences", type=int, default=5, help="Minimum daily factor occurrences for target triples")
    parser.add_argument("--test-fraction", type=float, default=0.2, help="Fraction of target triples for test")
    parser.add_argument("--validation-fraction", type=float, default=0.1, help="Fraction of target triples for validation")
    parser.add_argument("--recommendation-days", type=int, default=12, help="Number of days to generate recommendations for")
    parser.add_argument("--top-k", type=int, default=5, help="Recommendations per selected day")
    parser.add_argument("--quick", action="store_true", help="Use a very small grid for fast smoke testing")
    return parser.parse_args()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    args = parse_args()
    random.seed(RANDOM_SEED)
    np.random.seed(RANDOM_SEED)
    torch.manual_seed(RANDOM_SEED)

    output_dir = args.output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    tables = load_source_tables(args.db.resolve())
    target_triples = make_target_triples(tables["factor_daily"], args.min_occurrences)
    context_triples = make_context_triples(tables)
    train_targets, validation_targets, test_targets = split_target_triples(
        target_triples,
        test_fraction=args.test_fraction,
        validation_fraction=args.validation_fraction,
        seed=RANDOM_SEED,
    )
    training, validation, testing = make_triples_factories(
        context_triples,
        train_targets,
        validation_targets,
        test_targets,
    )

    LOGGER.info(
        "Prepared %s context triples and %s target triples: train=%s validation=%s test=%s",
        len(context_triples),
        len(target_triples),
        len(train_targets),
        len(validation_targets),
        len(test_targets),
    )

    target_triples.to_csv(output_dir / "target_triples_all.csv", index=False)
    train_targets.to_csv(output_dir / "target_triples_train.csv", index=False)
    validation_targets.to_csv(output_dir / "target_triples_validation.csv", index=False)
    test_targets.to_csv(output_dir / "target_triples_test.csv", index=False)

    results = [
        train_and_evaluate(config, training, validation, testing, RANDOM_SEED)
        for config in build_model_grid(args.quick)
    ]
    best = select_best_model(results)
    write_outputs(
        output_dir,
        results,
        best,
        {
            "context_triples": len(context_triples),
            "target_triples": len(target_triples),
            "train_target_triples": len(train_targets),
            "validation_target_triples": len(validation_targets),
            "test_target_triples": len(test_targets),
        },
    )

    factor_labels = (
        target_triples[["tail", "factor_label"]]
        .drop_duplicates()
        .set_index("tail")["factor_label"]
        .to_dict()
    )
    recommendations = generate_recommendations(
        best["pipeline_result"],
        training,
        sorted(factor_labels),
        factor_labels,
        train_targets,
        target_triples,
        make_day_context(tables),
        output_dir,
        args.recommendation_days,
        args.top_k,
    )

    LOGGER.info("Best model: %s (MRR=%s)", best["run_id"], best["mrr"])
    LOGGER.info("Model comparison written to %s", output_dir / "model_comparison.csv")
    LOGGER.info("Recommendations written to %s (%s rows)", output_dir / "factor_recommendations.csv", len(recommendations))


if __name__ == "__main__":
    main()
