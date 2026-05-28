# Knowledge Graph Embeddings: factor recommendation

## ML task

The KGE pipeline solves a link prediction task over the exploitation Knowledge Graph:

```text
(Day, hasLikelyContributingFactor, ContributingFactor)
```

The target relation is derived from the existing KG path:

```text
Day -> FactorCount -> ContributingFactor
```

Only factor links with at least 5 daily occurrences are used as positive examples. This removes very sparse/noisy factors and leaves a compact recommendation task.

## Why this is graph-based

This is not the same as the tabular regression/clustering from P1. The model learns embeddings for entities and relations in the KG, then ranks candidate factor nodes for each day node.

The training graph includes contextual KG relations such as:

| Context path | Purpose |
| --- | --- |
| `Day -> WeatherObservation` | Connects days to weather observations. |
| `Day -> MobilityObservation` | Connects days to estimated Uber activity. |
| `Day -> SafetyObservation -> Borough` | Adds borough-level safety structure. |
| `Day -> VehicleTypeCount -> VehicleType` | Adds vehicle-type graph context. |
| `Day -> weather/mobility/safety bins` | Adds categorical context nodes derived from KG literals. |

To avoid leakage, the original `hasFactorCount` and `aboutFactor` edges are excluded from the KGE training context. Otherwise, the target link would be almost directly visible as a two-hop path.

## Data split

The script creates the target triples and splits them into train/validation/test before training:

| Split item | Count |
| --- | ---: |
| Context triples | 18,174 |
| Target triples | 3,685 |
| Train target triples | 2,580 |
| Validation target triples | 368 |
| Test target triples | 737 |

The split keeps at least one training example for each factor and each day, so test entities are still known to the embedding model.

## Models and hyperparameters

The pipeline compares four KGE models with PyKEEN:

| Model | Intuition |
| --- | --- |
| TransE | Translational model: relation as vector translation from head to tail. |
| DistMult | Bilinear model with diagonal relation matrices. |
| ComplEx | Complex-valued bilinear model, more expressive for asymmetric patterns. |
| RotatE | Models relations as rotations in complex space. |

Small grid search:

| Model | Dimensions | Learning rate | Epochs | Negative samples |
| --- | ---: | ---: | ---: | ---: |
| TransE | 32 | 0.01 | 25 | 16 |
| TransE | 64 | 0.01 | 25 | 16 |
| TransE | 64 | 0.005 | 25 | 16 |
| DistMult | 64 | 0.01 | 25 | 16 |
| ComplEx | 64 | 0.01 | 25 | 16 |
| RotatE | 64 | 0.01 | 25 | 16 |

## Results

The comparison is saved in `outputs/kge/model_comparison.csv`.

| Model/run | MRR | Hits@1 | Hits@3 | Hits@10 |
| --- | ---: | ---: | ---: | ---: |
| RotatE dim64 lr0.01 | 0.6514 | 0.5292 | 0.7408 | 0.8521 |
| TransE dim32 lr0.01 | 0.4360 | 0.3094 | 0.4925 | 0.6934 |
| TransE dim64 lr0.01 | 0.4292 | 0.2972 | 0.4796 | 0.7198 |
| TransE dim64 lr0.005 | 0.3891 | 0.2626 | 0.4288 | 0.6560 |
| DistMult dim64 lr0.01 | 0.3131 | 0.2361 | 0.3263 | 0.4905 |
| ComplEx dim64 lr0.01 | 0.0792 | 0.0197 | 0.0726 | 0.1825 |

Best model: `RotatE_dim64_lr0.01_ep25_neg16`.

## Recommendations

The selected model ranks candidate `ContributingFactor` nodes for selected `Day` nodes. Results are saved in:

```text
outputs/kge/factor_recommendations.csv
```

Each row includes the day, recommended factor, model score, whether that factor was observed in the full KG, and basic day context such as average temperature, precipitation, estimated Uber trips, total collisions and top borough.

Example rows:

| Day | Recommended factor | Observed in full KG | Context |
| --- | --- | --- | --- |
| 2016-01-01 | DRIVER INEXPERIENCE | false | 38.0 avg temp, 393 collisions |
| 2016-01-01 | TURNING IMPROPERLY | true | 38.0 avg temp, 393 collisions |
| 2016-01-02 | TRAFFIC CONTROL DISREGARDED | true | 36.0 avg temp, 422 collisions |

Rows marked `was_observed_in_full_kg=true` are useful sanity checks: the model recovered factors that actually existed in the full KG but may have been hidden from training. Rows marked `false` are graph-based recommendations that can be interpreted as plausible factor candidates, not ground-truth claims.

## How to run

```powershell
.\.venv\Scripts\python.exe scripts\kge_factor_recommendation.py
```

Fast smoke test:

```powershell
.\.venv\Scripts\python.exe scripts\kge_factor_recommendation.py --quick --recommendation-days 3 --top-k 3
```

Main outputs:

| File | Description |
| --- | --- |
| `outputs/kge/model_comparison.csv` | Metrics for all KGE runs. |
| `outputs/kge/factor_recommendations.csv` | Top factor recommendations per selected day. |
| `outputs/kge/run_metadata.json` | Target relation, selected model and split sizes. |
| `outputs/kge/target_triples_*.csv` | All/train/validation/test target triples. |
| `outputs/kge/models/` | Saved PyKEEN artifacts for the best run. |
