# Project 2 Companion Report: Semantic Data Management with Knowledge Graphs

**Course:** Advanced Databases  
**Project:** BDA Practical Assignment 2  
**Authors:** Pablo Aviles, Joel Blanco, Marc Landa, Marina Teruel

## 1. Objective and relation with Project 1

This second project extends the DataOps pipeline developed in Project 1, but changes the main analytical goal. In Project 1, the final exploitation layer was tabular and the analysis was based on classical machine learning tasks: regression and clustering over daily accident, weather and Uber indicators. In Project 2, we reuse the same data engineering philosophy, but the exploitation layer is now semantic: the integrated data is represented as a Knowledge Graph (KG) in RDF/RDFS and exploited with SPARQL and Knowledge Graph Embeddings (KGE).

The goal is therefore not to repeat the previous tabular analysis, but to show that semantic modelling adds value. The KG makes explicit the relations between days, weather observations, mobility observations, borough-level safety observations, vehicle types and contributing factors. This enables graph-oriented analysis, such as pattern matching over paths and link prediction between days and plausible contributing factors.

The project is implemented with Python scripts instead of notebooks. The full execution is orchestrated by `run_pipeline.py`, while the main components are separated into specific scripts for the landing, formatted, trusted, exploitation, SPARQL and KGE stages.

## 2. Datasets

The project uses three datasets related to traffic conditions in New York City during 2016. A limitation identified in the feedback of Project 1 was that the datasets were not sufficiently presented. For this reason, in this report we explicitly describe what each dataset contributes, its size after ingestion, its granularity and its limitations.

**Uber NYC 2016.** The Uber dataset contains **1,474 rows and 10 columns** after ingestion into the Landing Zone. It describes Uber activity in New York City during 2016, including variables such as base license number, base name, pickup start and end dates, total dispatched trips and unique dispatched vehicles. Its original granularity is weekly and city/base-level, so it provides a general proxy for mobility demand rather than detailed borough-level movement.

**Weather NYC 2016.** The weather dataset contains **366 rows and 7 columns** after ingestion. It provides daily meteorological information for New York City, including maximum, minimum and average temperature, precipitation, snowfall and snow depth. Its granularity is daily and city-level, so each record represents the general weather conditions of the city for a specific day.

**NYC Vehicle Collisions.** The collisions dataset contains **477,732 rows and 29 columns** after ingestion. Each row represents an individual police-reported traffic collision, with information such as date, time, borough, location, number of injured or killed people, vehicle types and contributing factors. Its granularity is event-level, which makes it the most detailed dataset and the richest one for semantic modelling.

| Dataset | Rows after ingestion | Columns after ingestion | Granularity | Main limitation |
| --- | ---: | ---: | --- | --- |
| Uber NYC 2016 | 1,474 | 10 | Weekly, city/base-level | No borough-level movement. |
| Weather NYC 2016 | 366 | 7 | Daily, city-level | No borough-level weather. |
| NYC Vehicle Collisions | 477,732 | 29 | Individual collision event | Missing boroughs in some records. |

Since the datasets have different granularities, they are integrated through the only common and reliable dimension: the date. This allows weekly Uber activity, daily weather conditions and individual collision records to be aligned at a shared daily level without forcing unsupported spatial joins.

## 3. Pipeline architecture

The pipeline follows the zone-based architecture from Project 1, preserving the strengths that were positively evaluated: traceability, Parquet storage, DuckDB, Spark, denial constraints, quality reports and a single execution script.

| Zone | Main scripts / artifacts | Description |
| --- | --- | --- |
| Landing Zone | `scripts/data_collector.py`, `landing_zone/` | Downloads the Kaggle datasets, stores snapshots by execution date and converts CSV files to Parquet. This preserves traceability and improves I/O efficiency. Each dataset is processed independently, improving fault tolerance. |
| Formatted Zone | `scripts/formatted_zone.py`, `formatted_zone.db` | Converts Parquet snapshots into SQL-style tables using Spark and DuckDB. The objective is syntactic homogenization: column names, formats and basic type mapping. |
| Trusted Zone | `scripts/trusted_zone.py`, `trusted_zone.db` | Applies data quality rules, denial constraints, deduplication and cleaning. This stage produces trusted tables for accidents, weather and Uber. |
| KG-based Exploitation Zone | `scripts/exploitation_zone.py`, `exploitation_zone.db`, `exploitation_zone.ttl` | Builds the RDF/RDFS Knowledge Graph and stores both a DuckDB materialization and a Turtle serialization. |
| Analysis pipelines | `scripts/sparql_analysis.py`, `scripts/kge_factor_recommendation.py`, `queries/`, `outputs/` | Executes semantic analysis with SPARQL and link prediction with Knowledge Graph Embeddings. |

The final `exploitation_zone.db` contains analysis tables and KG tables: `kg_nodes`, `kg_edges`, `kg_literals` and `kg_schema_properties`. The file `exploitation_zone.ttl` serializes the same graph in Turtle, including the RDFS schema.

## 4. Knowledge Graph model

The KG uses `Day` as the central entity because it is the only dimension shared by all sources. A day can be connected to weather, mobility, safety, vehicle and factor information without forcing artificial joins that are not supported by the data.

| KG component | Meaning |
| --- | --- |
| `Day` | Calendar day in 2016. It is the semantic hub of the graph. |
| `WeatherObservation` | Daily weather observation connected to a day. |
| `MobilityObservation` | Estimated daily Uber activity connected to a day. |
| `SafetyObservation` | Borough-level daily accident summary. |
| `Borough` | NYC borough, including `UNKNOWN` when the original data lacks borough information. |
| `ContributingFactor` | Semantic entity representing an accident contributing factor. |
| `VehicleType` | Semantic entity representing a vehicle type involved in accidents. |
| `FactorCount` | Intermediate node linking a day to a contributing factor with a count. |
| `VehicleTypeCount` | Intermediate node linking a day to a vehicle type with a count. |

`FactorCount` and `VehicleTypeCount` are modelled as intermediate nodes because the relation between a day and a factor or vehicle type is not simply binary. It has an associated value: the number of occurrences on that day. In RDF, this is better represented through a count node than by attaching a literal directly to an edge.

The RDFS schema declares classes, subclasses, domains and ranges. For example, `hasWeatherObservation` has domain `Day` and range `WeatherObservation`. This improves the interpretability of the graph and allows type inference from property usage.

## 5. SPARQL analysis pipeline

The SPARQL pipeline is not a repetition of the regression or clustering work from Project 1. Instead, it performs semantic pattern matching over graph paths. The queries are stored in `queries/`, and results are written to `outputs/sparql/`.

The main queries are:

| Query | Analysis |
| --- | --- |
| `sparql_smoke_test.sparql` | Validates that the KG loads correctly and that days can be joined with weather, mobility, borough safety observations and collisions. |
| `01_high_collision_days_contributing_factors.sparql` | Finds contributing factors that appear most often in borough-days with high collision counts. |
| `02_adverse_weather_vehicle_types.sparql` | Ranks vehicle types appearing on days with adverse weather conditions such as precipitation or snow. |
| `03_weekend_vs_weekday_factor_profile.sparql` | Compares contributing factor profiles between weekends and weekdays. The number of weekend and weekday days is computed from the KG, not hardcoded. |
| `04_high_mobility_borough_collision_risk.sparql` | Identifies boroughs with higher collision levels on days with above-average Uber mobility. |
| `05_weather_factor_vehicle_paths.sparql` | Shows explicit paths connecting weather, factors and vehicle types through the same day. |

These queries demonstrate why the KG representation is useful. For instance, a query can traverse from a `Day` to a `WeatherObservation`, then to a `VehicleTypeCount`, then to a `VehicleType`, while also considering borough-level safety observations. This is more natural as graph pattern matching than as a manually assembled flat table.

## 6. Knowledge Graph Embeddings pipeline

The second analysis pipeline uses Knowledge Graph Embeddings for link prediction and recommendation. The task is to rank the most plausible contributing factors for a given day:

```text
(Day, hasLikelyContributingFactor, ContributingFactor)
```

This relation is derived from the existing KG path:

```text
Day -> FactorCount -> ContributingFactor
```

However, to avoid leakage, the original `hasFactorCount` and `aboutFactor` relations are excluded from the training context. If they were kept, the model could recover the target relation almost directly from a two-hop path. Instead, the model learns from other graph context: weather, mobility, safety observations, borough information, vehicle types and derived categorical context nodes.

The implementation is in `scripts/kge_factor_recommendation.py`. It prepares triples, creates train/validation/test splits, trains several KGE models with PyKEEN, evaluates them with link prediction metrics and writes recommendations to `outputs/kge/factor_recommendations.csv`.

The compared models are TransE, DistMult, ComplEx and RotatE. A small grid search is used to avoid choosing the model by intuition only, while keeping runtime reasonable for the project scale.

| Model/run | MRR | Hits@1 | Hits@3 | Hits@10 |
| --- | ---: | ---: | ---: | ---: |
| `RotatE_dim64_lr0.01_ep25_neg16` | 0.6514 | 0.5292 | 0.7408 | 0.8521 |
| `TransE_dim32_lr0.01_ep25_neg16` | 0.4360 | 0.3094 | 0.4925 | 0.6934 |
| `TransE_dim64_lr0.01_ep25_neg16` | 0.4292 | 0.2972 | 0.4796 | 0.7198 |
| `TransE_dim64_lr0.005_ep25_neg16` | 0.3891 | 0.2626 | 0.4288 | 0.6560 |
| `DistMult_dim64_lr0.01_ep25_neg16` | 0.3131 | 0.2361 | 0.3263 | 0.4905 |
| `ComplEx_dim64_lr0.01_ep25_neg16` | 0.0792 | 0.0197 | 0.0726 | 0.1825 |

The best model is RotatE. Its Hits@10 of 0.8521 means that, in approximately 85% of test cases, the correct contributing factor appears within the top 10 ranked candidates. This is a graph-native result: the model recommends factor nodes for day nodes using learned embeddings of the KG structure.

The split sizes were:

| Item | Count |
| --- | ---: |
| Context triples | 18,174 |
| Target triples | 3,685 |
| Train target triples | 2,580 |
| Validation target triples | 368 |
| Test target triples | 737 |

The recommendation output includes the day, recommended factor, model score, whether the factor was observed in the full KG, and basic context such as temperature, precipitation, estimated Uber trips, total collisions and top borough. These recommendations should be interpreted as plausible factors to monitor, not as causal explanations.

## 7. Main results

The SPARQL results show that the KG supports interpretable semantic analysis across sources. We can query patterns involving adverse weather, mobility, borough-level collision risk, contributing factors and vehicle types without flattening the whole graph into a single table.

The KGE results show that the graph also supports predictive analysis. RotatE clearly outperformed the other tested models, achieving MRR = 0.6514 and Hits@10 = 0.8521. This suggests that the graph structure contains useful signal for recommending contributing factors associated with a day.

Together, SPARQL and KGE cover two complementary forms of semantic exploitation: explicit graph pattern querying and latent graph-based prediction.

## 8. Limitations

The project has several limitations that must be considered when interpreting the results:

- The data covers only 2016, so temporal generalization to other years is not evaluated.
- Uber and weather are city-level sources, while accidents include borough information. Therefore, borough-level conclusions mainly come from the accident dataset.
- Uber daily activity is estimated from weekly records using a distribution strategy, not directly observed.
- Some accidents have unknown boroughs, which are preserved as `UNKNOWN` to avoid losing data.
- KGE recommendations are not causal explanations and should not be interpreted as direct accident prevention rules. They are plausible factor rankings based on KG structure.

## 9. Conclusion

Project 1 built a strong DataOps foundation: traceable ingestion, Parquet storage, Spark and DuckDB processing, trusted data quality checks, denial constraints and orchestrated execution. Project 2 keeps that foundation but changes the exploitation paradigm.

Instead of producing only a flat analytical table, the exploitation zone now produces a Knowledge Graph in RDF/RDFS. This graph makes the semantics of the data explicit and allows two graph-oriented analysis pipelines. SPARQL is used for interpretable pattern matching over graph paths, while Knowledge Graph Embeddings are used for link prediction and factor recommendation.

The main difference is therefore clear: Project 1 focused on tabular exploitation with regression and clustering, while Project 2 focuses on semantic exploitation with a Knowledge Graph, SPARQL queries and KGE-based recommendations.
