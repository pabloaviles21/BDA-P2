# SPARQL analysis queries

These queries are the first graph-oriented analysis pipeline for the exploitation zone.
They keep `sparql_smoke_test.sparql` as a basic validation query, then add analysis queries
that follow KG paths between days, observations, boroughs, contributing factors and vehicle types.

Run one query:

```powershell
.\.venv\Scripts\python.exe scripts\sparql_analysis.py --query queries\01_high_collision_days_contributing_factors.sparql
```

Run all queries and save CSV outputs:

```powershell
.\.venv\Scripts\python.exe scripts\sparql_analysis.py --query queries --output-dir outputs\sparql
```

## Query catalogue

| File | Analysis goal |
| --- | --- |
| `sparql_smoke_test.sparql` | Validates that the KG loads and joins days with weather, mobility, borough safety observations and collisions. |
| `01_high_collision_days_contributing_factors.sparql` | Finds contributing factors that appear most on borough-days with high collisions. |
| `02_adverse_weather_vehicle_types.sparql` | Ranks vehicle types present on days with precipitation or snow. |
| `03_weekend_vs_weekday_factor_profile.sparql` | Compares contributing-factor profiles between weekends and weekdays. |
| `04_high_mobility_borough_collision_risk.sparql` | Identifies boroughs with higher collision levels on high Uber mobility days. |
| `05_weather_factor_vehicle_paths.sparql` | Shows explicit KG paths linking adverse weather, factors and vehicle types through the same day. |
