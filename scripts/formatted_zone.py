"""
Formatted Zone Pipeline: Schema Harmonization & Typed Persistence

Reads the latest landing snapshots, standardizes column names, applies the
type conversions required by the project, and persists the result as
relational tables in DuckDB.

Key Architecture:
- Snapshot Resolution: each dataset is loaded from its latest landing version.
- Logical Formatting: schema normalization and typing start here.
- Typed Storage: curated tables are materialized in DuckDB for downstream zones.
"""

from datetime import datetime
from pathlib import Path
import re
import urllib.request

import duckdb
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr


BASE_DIR = Path(__file__).resolve().parents[1]
LANDING_ZONE_DIR = BASE_DIR / "landing_zone"
DUCKDB_JAR_PATH = BASE_DIR / "duckdb.jar"
FALLBACK_DUCKDB_JAR_PATHS = [
    BASE_DIR.parent / "duckdb.jar",
    BASE_DIR.parent / "duckdb_jdbc.jar",
]

DATASETS = {
    "uber_trips": {
        "table_name": "uber_data",
        "date_columns": {
            "pickup_start_date": "%m/%d/%Y",
            "pickup_end_date": "%m/%d/%Y",
        },
        "integer_columns": [
            "wave_number",
            "years",
            "week_number",
            "total_dispatched_trips",
            "unique_dispatched_vehicle",
        ],
    },
    "weather_nyc": {
        "table_name": "weather_data",
        "date_columns": {
            "date": "%d-%m-%Y",
        },
        "double_columns": [
            "maximum_temperature",
            "minimum_temperature",
            "average_temperature",
            "precipitation",
            "snow_fall",
            "snow_depth",
        ],
    },
    "accidents_nyc": {
        "table_name": "accidents_data",
        "date_columns": {
            "date": "%m/%d/%Y",
        },
        "time_columns": {
            "time": "%H:%M",
        },
        "integer_columns": [
            "persons_injured",
            "persons_killed",
            "pedestrians_injured",
            "pedestrians_killed",
            "cyclists_injured",
            "cyclists_killed",
            "motorists_injured",
            "motorists_killed",
        ],
        "double_columns": [
            "latitude",
            "longitude",
        ],
    },
}


def sanitize_column_name(column_name):
    """
    Normalizes source headers into a predictable snake_case convention.
    """
    normalized = column_name.strip().lower()
    normalized = re.sub(r"[^a-z0-9]+", "_", normalized)
    normalized = re.sub(r"_+", "_", normalized)
    return normalized.strip("_")


def list_available_dates(dataset_name):
    """
    Lists all valid execution-date partitions available for a landing dataset.
    """
    dataset_root = LANDING_ZONE_DIR / dataset_name
    available_dates = []

    if dataset_root.is_dir():
        for entry in dataset_root.iterdir():
            if not entry.is_dir():
                continue

            try:
                datetime.strptime(entry.name, "%Y-%m-%d")
                available_dates.append(entry.name)
            except ValueError:
                continue

    return sorted(set(available_dates))


def resolve_dataset_path(dataset_name):
    """
    Resolves the latest landing snapshot to keep the formatted zone reproducible
    while still following the newest successful ingestion.
    """
    available_dates = list_available_dates(dataset_name)
    if not available_dates:
        return None, None

    latest_date = available_dates[-1]
    return latest_date, LANDING_ZONE_DIR / dataset_name / latest_date


def list_parquet_files(dataset_path):
    return sorted(path for path in dataset_path.iterdir() if path.suffix.lower() == ".parquet")


def map_source_format_to_spark(fmt):
    """
    Converts Python-like date patterns into the Spark SQL equivalents used by
    `try_to_date` and `try_to_timestamp`.
    """
    return (
        fmt.replace("%m", "M")
        .replace("%d", "d")
        .replace("%Y", "yyyy")
        .replace("%H", "H")
        .replace("%M", "mm")
    )


def format_dataframe(dataframe, dataset_name):
    """
    Applies schema harmonization and the dataset-specific type conversions
    expected by the downstream trusted and exploitation zones.
    """
    dataset_config = DATASETS[dataset_name]

    for original_name in dataframe.columns:
        standardized_name = sanitize_column_name(original_name)
        dataframe = dataframe.withColumnRenamed(original_name, standardized_name)

    columns = dataframe.columns

    # Casting is intentionally declarative: only columns defined in the dataset
    # configuration are converted so the formatting logic stays easy to audit.
    for column_name in columns:
        if column_name in dataset_config.get("date_columns", {}):
            spark_format = map_source_format_to_spark(
                dataset_config["date_columns"][column_name]
            )
            dataframe = dataframe.withColumn(
                column_name,
                expr(f"try_to_date(`{column_name}`, '{spark_format}')"),
            )

        elif column_name in dataset_config.get("time_columns", {}):
            if dataset_name == "accidents_nyc" and column_name == "time":
                # In accidents, the original time is later reused to build a
                # combined collision timestamp, so blank strings are cleaned but
                # the value remains textual at this stage.
                dataframe = dataframe.withColumn(
                    column_name,
                    expr(f"NULLIF(TRIM(`{column_name}`), '')"),
                )
            else:
                spark_format = map_source_format_to_spark(
                    dataset_config["time_columns"][column_name]
                )
                dataframe = dataframe.withColumn(
                    column_name,
                    expr(f"try_to_timestamp(`{column_name}`, '{spark_format}')"),
                )

        elif column_name in dataset_config.get("integer_columns", []):
            dataframe = dataframe.withColumn(
                column_name,
                expr(f"try_cast(`{column_name}` as int)"),
            )

        elif column_name in dataset_config.get("double_columns", []):
            dataframe = dataframe.withColumn(
                column_name,
                expr(f"try_cast(`{column_name}` as double)"),
            )

    if dataset_name == "accidents_nyc" and "date" in columns and "time" in columns:
        # This derived timestamp is useful later for analytics and data-quality
        # checks, but it is still computed from the original event attributes.
        dataframe = dataframe.withColumn(
            "collision_timestamp",
            expr("try_to_timestamp(concat(date_format(date, 'yyyy-MM-dd'), ' ', `time`))"),
        )

    return dataframe


def ensure_duckdb_driver():
    """
    Downloads the DuckDB JDBC jar only when it is not already present locally.
    """
    if DUCKDB_JAR_PATH.exists():
        return DUCKDB_JAR_PATH

    for fallback_path in FALLBACK_DUCKDB_JAR_PATHS:
        if fallback_path.exists():
            return fallback_path

    print("DuckDB JDBC driver")
    urllib.request.urlretrieve(
        "https://repo1.maven.org/maven2/org/duckdb/duckdb_jdbc/0.10.1/duckdb_jdbc-0.10.1.jar",
        str(DUCKDB_JAR_PATH),
    )
    return DUCKDB_JAR_PATH


def create_spark_session(jar_path):
    """
    Builds the Spark session used for formatting and type conversion.
    """
    spark = (
        SparkSession.builder
        .appName("FormattedZonePipeline")
        # On Windows, using driver.extraClassPath is more stable here than
        # relying on spark.jars for this DuckDB integration.
        .config("spark.driver.extraClassPath", str(jar_path))
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "false")
    return spark


def initialize_database(db_path):

    if db_path.is_file():
        db_path.unlink()

    connection = duckdb.connect(str(db_path))
    connection.close()


def write_table_to_duckdb(dataframe, table_name, db_path):
    """
    Persists the formatted Spark dataframe into DuckDB through an intermediate
    pandas dataframe, which is the most stable path in this Windows setup.
    """
    pandas_frame = dataframe.toPandas()

    with duckdb.connect(str(db_path)) as connection:
        connection.register("tmp_df", pandas_frame)
        connection.execute(
            f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM tmp_df"
        )
        connection.unregister("tmp_df")


def run_data_formatting_pipeline(db_path="formatted_zone.db"):
    """
    Orchestrates the formatted zone: latest landing snapshot resolution, schema
    normalization, type conversion, and final persistence in DuckDB.
    """
    print("Starting Formatted Zone pipeline with Spark and DuckDB")

    database_path = BASE_DIR / db_path
    jar_path = ensure_duckdb_driver()
    initialize_database(database_path)
    spark = create_spark_session(jar_path)

    try:
        for dataset_name, dataset_config in DATASETS.items():
            resolved_date, dataset_path = resolve_dataset_path(dataset_name)
            print(f"--- Processing dataset: {dataset_name} ---")

            if dataset_path is None:
                print(f"Skipping {dataset_name}: no landing snapshot was found.")
                continue

            parquet_files = list_parquet_files(dataset_path)
            dataframe = spark.read.parquet(*[str(path) for path in parquet_files])
            formatted_dataframe = format_dataframe(dataframe, dataset_name)

            table_name = dataset_config["table_name"]
            write_table_to_duckdb(formatted_dataframe, table_name, database_path)

            print(
                f"Table {table_name} created successfully from snapshot {resolved_date}.\n"
            )
    finally:
        spark.stop()


if __name__ == "__main__":
    run_data_formatting_pipeline()
    import os
    os._exit(0)
