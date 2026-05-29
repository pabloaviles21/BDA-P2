"""
Landing Zone Pipeline: Data Ingestion & Collector

Entry point for the Data Engineering pipeline. Authenticates via Kaggle API, 
downloads raw datasets, and stores them in an optimized local format.

Key Architecture:
- Traceability: Snapshots are partitioned by execution date (YYYY-MM-DD).
- Physical Transformation: CSVs are converted to Parquet for storage and 
  read efficiency. No logical schema curation is applied.
- Fault Tolerance: Datasets are processed independently to prevent pipeline blocks.
"""

import argparse
from datetime import datetime
from pathlib import Path

import pandas as pd
from kaggle.api.kaggle_api_extended import KaggleApi


BASE_DIR = Path(__file__).resolve().parents[1]
LANDING_ZONE_DIR = BASE_DIR / "landing_zone"

DATASETS = {
    "danvargg/uber-nyc-2016": "uber_trips",
    "mathijs/weather-data-in-new-york-city-2016": "weather_nyc",
    "nypd/vehicle-collisions": "accidents_nyc",
}


def parse_execution_date(value):
    """
    Validates and formats the execution date, defaulting to the current day.
    """
    if value is None:
        return datetime.now().strftime("%Y-%m-%d")

    try:
        return datetime.strptime(value, "%Y-%m-%d").strftime("%Y-%m-%d")
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            "Execution date must use YYYY-MM-DD format."
        ) from exc


def build_output_path(dataset_folder, execution_date):
    # Raw snapshots are partitioned by execution date so reruns remain traceable.
    return LANDING_ZONE_DIR / dataset_folder / execution_date


def iter_csv_files(root_path):
    # Kaggle packages are not consistent across datasets, so CSVs are discovered
    # recursively instead of assuming a fixed internal layout after extraction.
    return sorted(path for path in root_path.rglob("*.csv") if path.is_file())


def convert_csv_to_parquet(dataset_path):
    converted_files = []

    for csv_path in iter_csv_files(dataset_path):
        parquet_path = csv_path.with_suffix(".parquet")
        print(f"Converting {csv_path.name} to Parquet...")

        # The landing zone keeps the raw content as close as possible to the source.
        # At this stage the conversion is purely physical: CSV is rewritten as Parquet
        # for lighter storage and faster downstream reads, without schema curation.
        dataframe = pd.read_csv(csv_path, low_memory=False)
        dataframe.to_parquet(parquet_path, engine="pyarrow", index=False)

        # The original CSV is removed only after the Parquet file is created
        # successfully, which avoids leaving partially replaced snapshots behind.
        csv_path.unlink()
        converted_files.append(parquet_path.name)

    return converted_files


def run_data_collector(execution_date=None):
    """
    Orchestrates the ingestion process: authenticates, downloads, extracts, 
    and converts datasets independently.
    """
    execution_date = parse_execution_date(execution_date)

    api = KaggleApi()
    api.authenticate()
    print("Kaggle authentication completed.")

    processed = []
    failed = []

    for slug, dataset_folder in DATASETS.items():
        dataset_path = build_output_path(dataset_folder, execution_date)
        dataset_path.mkdir(parents=True, exist_ok=True)
        print(f"\n--- Processing {dataset_folder} ({execution_date}) ---")

        try:
            # Each dataset is handled independently so one failed download does not
            # block the remaining landing snapshots for the same execution date.
            api.dataset_download_files(slug, path=str(dataset_path), unzip=True)
            converted_files = convert_csv_to_parquet(dataset_path)
            processed.append(dataset_folder)

            print(f"Stored in: {dataset_path}")
            print(f"Parquet files created: {len(converted_files)}")
        except Exception as exc:
            failed.append(dataset_folder)
            print(f"Download failed for {dataset_folder}: {exc}")

    print("\nExecution summary")
    print(f"Successful datasets: {len(processed)}")
    print(f"Failed datasets: {len(failed)}")

    if failed:
        print("Failed:", ", ".join(failed))

    return {
        "execution_date": execution_date,
        "processed": processed,
        "failed": failed,
    }


def parse_args():
    """
    Parses CLI arguments to allow manual execution date overrides.
    """
    parser = argparse.ArgumentParser(
        description="Download Kaggle datasets into landing_zone/<dataset>/<YYYY-MM-DD>/."
    )
    parser.add_argument(
        "--date",
        dest="execution_date",
        type=parse_execution_date,
        # Accepting an explicit execution date makes the landing snapshots easier
        # to reproduce in demos, reruns, or late project integration.
        help="Execution date in YYYY-MM-DD format. Defaults to the current date.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run_data_collector(execution_date=args.execution_date)
