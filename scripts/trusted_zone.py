"""
Trusted Zone Pipeline - Data Quality Assessment and Cleaning

This module implements data quality checks and cleaning processes for the Trusted Zone.
It applies Denial Constraints to identify and remove invalid records across three datasets:
- uber_data
- weather_data
- accidents_data

All processing is performed using Apache Spark and SparkSQL.
"""

import logging
import sys
from typing import Dict, List, Tuple
from datetime import datetime

import duckdb
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import expr

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Database paths
FORMATTED_DB_PATH = "formatted_zone.db"
TRUSTED_DB_PATH = "trusted_zone.db"

# Here we define the Denial Constraints for each dataset
DENIAL_CONSTRAINTS = {
    "uber_data": {
        "table_name": "uber_data",
        "constraints": [
            {
                "name": "wave_number_positive",
                "description": "Wave number must be positive (> 0)",
                "violation_condition": "wave_number <= 0",
            },
            {
                "name": "dispatched_trips_consistency",
                "description": "Total dispatched trips must be >= unique dispatched vehicles",
                "violation_condition": "total_dispatched_trips < unique_dispatched_vehicle",
            },
            {
                "name": "date_range_validity",
                "description": "Pickup start date must be <= pickup end date",
                "violation_condition": "pickup_start_date > pickup_end_date",
            },
            {
                "name": "week_number_valid_range",
                "description": "Week number must be between 1 and 52",
                "violation_condition": "week_number < 1 OR week_number > 52",
            },
        ],
    },
    "weather_data": {
        "table_name": "weather_data",
        "constraints": [
            {
                "name": "temperature_logical_order",
                "description": "Min temperature must be <= Average <= Max temperature",
                "violation_condition": "minimum_temperature > average_temperature OR average_temperature > maximum_temperature",
            },
            {
                "name": "precipitation_non_negative",
                "description": "Precipitation must be non-negative",
                "violation_condition": "precipitation < 0",
            },
            {
                "name": "snow_fall_non_negative",
                "description": "Snow fall must be non-negative",
                "violation_condition": "snow_fall < 0",
            },
            {
                "name": "snow_depth_consistency",
                "description": "Snow depth must be non-negative and realistic",
                "violation_condition": "snow_depth < 0 OR snow_depth > 1000",
            },
        ],
    },
    "accidents_data": {
        "table_name": "accidents_data",
        "constraints": [
            {
                "name": "injury_categories_sum",
                "description": "Total injured must equal sum of injured by category",
                "violation_condition": "persons_injured != (pedestrians_injured + cyclists_injured + motorists_injured)",
            },
            {
                "name": "death_categories_sum",
                "description": "Total killed must equal sum of killed by category",
                "violation_condition": "persons_killed != (pedestrians_killed + cyclists_killed + motorists_killed)",
            },
            {
                "name": "location_validity",
                "description": "NYC coordinates: latitude between 40.5-40.9, longitude between -74.3--73.7",
                "violation_condition": "(latitude IS NOT NULL AND (latitude < 40.5 OR latitude > 40.9)) OR (longitude IS NOT NULL AND (longitude < -74.3 OR longitude > -73.7))",
            },
            {
                "name": "geolocation_completeness_consistency",
                "description": "Latitude, longitude and location must be consistently present or absent together",
                "violation_condition": "(latitude IS NULL AND longitude IS NOT NULL) OR (latitude IS NOT NULL AND longitude IS NULL) OR ((latitude IS NULL OR longitude IS NULL) AND location IS NOT NULL) OR (latitude IS NOT NULL AND longitude IS NOT NULL AND location IS NULL)",
            },
            {
                "name": "non_negative_injuries",
                "description": "All injury counts must be non-negative",
                "violation_condition": "persons_injured < 0 OR pedestrians_injured < 0 OR cyclists_injured < 0 OR motorists_injured < 0",
            },
        ],
    },
}



# Here wew define a series of functions to implement the Trusted Zone pipeline stages
ACCIDENTS_STRING_COLUMNS = [
    "borough",
    "location",
    "on_street_name",
    "cross_street_name",
    "off_street_name",
    "vehicle_1_type",
    "vehicle_2_type",
    "vehicle_3_type",
    "vehicle_4_type",
    "vehicle_5_type",
    "vehicle_1_factor",
    "vehicle_2_factor",
    "vehicle_3_factor",
    "vehicle_4_factor",
    "vehicle_5_factor",
]

# Function to create a Spark session with optimized configurations for our workload
def create_spark_session() -> SparkSession:
    try:
        spark = SparkSession.builder \
            .appName("TrustedZonePipeline") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.sql.shuffle.partitions", "4") \
            .config("spark.driver.memory", "2g") \
            .config("spark.sql.execution.arrow.enabled", "true") \
            .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
            .getOrCreate()
        
        spark.sparkContext.setLogLevel("WARN")
        logger.info("✓ Spark session created successfully")
        return spark
    
    except Exception as e:
        logger.error(f"✗ Failed to create Spark session: {str(e)}")
        raise RuntimeError(f"Spark session creation failed: {str(e)}")

# Funtion to standardize optional string fields and add completeness flags for the accidents_data dataset
def standardize_accidents_dataset(spark_df: DataFrame) -> DataFrame:
    for column_name in ACCIDENTS_STRING_COLUMNS:
        if column_name in spark_df.columns:
            spark_df = spark_df.withColumn(
                column_name,
                expr(f"NULLIF(TRIM(`{column_name}`), '')")
            )

    return (
        spark_df
        .withColumn("has_borough", expr("borough IS NOT NULL"))
        .withColumn("has_zip_code", expr("zip_code IS NOT NULL"))
        .withColumn(
            "has_geo",
            expr("latitude IS NOT NULL AND longitude IS NOT NULL AND location IS NOT NULL")
        )
        .withColumn(
            "has_street_reference",
            expr("on_street_name IS NOT NULL OR cross_street_name IS NOT NULL OR off_street_name IS NOT NULL")
        )
        .withColumn("has_vehicle_1_type", expr("vehicle_1_type IS NOT NULL"))
        .withColumn("has_vehicle_1_factor", expr("vehicle_1_factor IS NOT NULL"))
        .withColumn(
            "location_detail_type",
            expr(
                """
                CASE
                    WHEN on_street_name IS NOT NULL AND cross_street_name IS NOT NULL THEN 'intersection'
                    WHEN off_street_name IS NOT NULL THEN 'off_street'
                    WHEN latitude IS NOT NULL AND longitude IS NOT NULL THEN 'geo_only'
                    ELSE 'missing'
                END
                """
            )
        )
    )

#Function where we read from the Formatted Zone (DuckDB) and convert to Spark DataFrame
def read_from_formatted_zone(table_name: str) -> DataFrame:
    try:
        # Connect to DuckDB and read table
        logger.info(f"  → Reading table '{table_name}' from {FORMATTED_DB_PATH}...")
        conn = duckdb.connect(FORMATTED_DB_PATH, read_only=True)
        
        # Verify table exists
        tables = conn.execute("SELECT table_name FROM information_schema.tables").fetchall()
        table_names = [t[0] for t in tables]
        
        if table_name not in table_names:
            raise ValueError(f"Table '{table_name}' not found in {FORMATTED_DB_PATH}. "
                           f"Available tables: {table_names}")
        
        # Read using df() method which returns a proper Pandas DataFrame
        logger.info(f"  → Reading from DuckDB...")
        pandas_df = conn.execute(f"SELECT * FROM {table_name}").df()
        record_count = len(pandas_df)
        conn.close()
        
        # Get Spark session and convert from Pandas
        logger.info(f"  → Converting to Spark DataFrame...")
        spark = SparkSession.getActiveSession()
        spark_df = spark.createDataFrame(pandas_df)
        
        logger.info(f"  ✓ Loaded {record_count:,} records from '{table_name}'")
        
        return spark_df
    
    except Exception as e:
        logger.error(f"✗ Failed to read '{table_name}' from Formatted Zone: {str(e)}")
        raise

# Function to save cleaned Spark DataFrame to the Trusted Zone (DuckDB)
def save_to_trusted_zone(spark_df: DataFrame, table_name: str) -> None:
    try:
        logger.info(f"  → Converting Spark DataFrame to Pandas for '{table_name}'...")
        pandas_df = spark_df.toPandas()
        
        logger.info(f"  → Writing '{table_name}' to {TRUSTED_DB_PATH}...")
        conn = duckdb.connect(TRUSTED_DB_PATH)
        conn.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM pandas_df")
        conn.close()
        
        record_count = len(pandas_df)
        logger.info(f"  ✓ Saved {record_count:,} clean records to '{table_name}' in Trusted Zone")
    
    except Exception as e:
        logger.error(f"✗ Failed to save '{table_name}' to Trusted Zone: {str(e)}")
        raise


# Now we define the core functions for data quality assessment and cleaning using Denial Constraints
# First, we assess data quality by counting violations of each constraint and printing a report
def assess_data_quality(spark_df: DataFrame, dataset_key: str) -> Dict[str, int]:
    if dataset_key not in DENIAL_CONSTRAINTS:
        raise KeyError(f"Dataset '{dataset_key}' not found in DENIAL_CONSTRAINTS configuration")
    
    constraints = DENIAL_CONSTRAINTS[dataset_key]["constraints"]
    
    # Cache the dataframe to avoid re-reading
    spark_df.cache()
    
    # Count total records once
    try:
        total_records = spark_df.count()
    except Exception as e:
        logger.error(f"✗ Error counting total records: {str(e)}")
        spark_df.unpersist()
        raise
    
    violations_report = {}
    
    logger.info(f"\n" + "=" * 80)
    logger.info(f" DATA QUALITY ASSESSMENT: {dataset_key.upper()}")
    logger.info(f"=" * 80)
    logger.info(f"Total records to assess: {total_records:,}")
    logger.info(f"Total constraints: {len(constraints)}")
    logger.info(f"{'-' * 80}")
    
    for i, constraint in enumerate(constraints, 1):
        constraint_name = constraint["name"]
        violation_condition = constraint["violation_condition"]
        description = constraint["description"]
        
        try:
            # Count records that violate the constraint
            violation_count = spark_df.filter(violation_condition).count()
            violations_report[constraint_name] = violation_count
            
            # Calculate percentage
            violation_percentage = (violation_count / total_records * 100) if total_records > 0 else 0
            
            # Print formatted report
            status = "✗ VIOLATIONS FOUND" if violation_count > 0 else "✓ PASSED"
            logger.info(f"[{i}/{len(constraints)}] {status}")
            logger.info(f"   Constraint: {constraint_name}")
            logger.info(f"   Description: {description}")
            logger.info(f"   Violations: {violation_count:,} ({violation_percentage:.2f}%)")
            logger.info(f"{'-' * 80}")
        
        except Exception as e:
            logger.error(f"   ✗ Error evaluating constraint '{constraint_name}': {str(e)}")
            violations_report[constraint_name] = -1  # Indicate evaluation error
    
    # Summary statistics
    total_violations = sum([v for v in violations_report.values() if v >= 0])
    clean_records = total_records - total_violations
    quality_score = (clean_records / total_records * 100) if total_records > 0 else 0
    
    logger.info(f" SUMMARY:")
    logger.info(f"   Total violations across all constraints: {total_violations:,}")
    logger.info(f"   Clean records (no violations): {clean_records:,}")
    logger.info(f"   Data quality score: {quality_score:.2f}%")
    logger.info(f"=" * 80 + "\n")
    
    # Unpersist cache
    spark_df.unpersist()
    
    return violations_report



#Here we perform the cleaning by applying the inverse of all constraints to keep only valid records, and then we remove duplicates
def clean_dataset(spark_df: DataFrame, dataset_key: str) -> DataFrame:
    if dataset_key not in DENIAL_CONSTRAINTS:
        raise KeyError(f"Dataset '{dataset_key}' not found in DENIAL_CONSTRAINTS configuration")
    
    constraints = DENIAL_CONSTRAINTS[dataset_key]["constraints"]
    
    # Cache to avoid re-reading during filtering
    spark_df.cache()
    initial_count = spark_df.count()
    
    logger.info(f"\n CLEANING: {dataset_key.upper()}")
    logger.info(f"Initial record count: {initial_count:,}")
    logger.info(f"Applying {len(constraints)} constraints...")
    
    # Build the valid records filter (inverse of all violations)
    valid_filter = "1=1"  # Start with a no-op condition
    
    for constraint in constraints:
        violation_condition = constraint["violation_condition"]
        constraint_name = constraint["name"]
        
        # Inverse logic: NOT(violation_condition)
        # We need to wrap it properly with parentheses
        inverse_condition = f"NOT({violation_condition})"
        valid_filter = f"({valid_filter}) AND ({inverse_condition})"
    
    # Apply the combined filter to keep only valid records
    cleaned_df = spark_df.filter(valid_filter)
    
    # Cache before removing duplicates
    cleaned_df.cache()
    
    # Remove duplicate records
    cleaned_df = cleaned_df.dropDuplicates()
    
    final_count = cleaned_df.count()
    removed_count = initial_count - final_count
    removal_percentage = (removed_count / initial_count * 100) if initial_count > 0 else 0
    
    logger.info(f"Records after quality filtering: {final_count:,}")
    logger.info(f"Records removed (invalid): {removed_count:,} ({removal_percentage:.2f}%)")
    logger.info(f"Data quality improved: {final_count:,} valid records retained ✓")
    
    # Unpersist original cached df
    spark_df.unpersist()
    
    return cleaned_df


def prepare_dataset_for_trusted_zone(spark_df: DataFrame, dataset_key: str) -> DataFrame:
    """
    Apply dataset-specific standardization before quality assessment and cleaning.
    """
    if dataset_key == "accidents_data":
        logger.info("  → Standardizing accidents_data optional fields and completeness flags...")
        return standardize_accidents_dataset(spark_df)

    return spark_df



# We define a main function to execute the entire Trusted Zone pipeline, which will be called from run_pipeline.py
# Here we use all the functions previously defined
def execute_trusted_zone_pipeline() -> None:
    spark = None
    try:
        # STAGE 1: Initialize Spark Session
        logger.info("\n" + "=" * 80)
        logger.info("TRUSTED ZONE PIPELINE - INITIALIZATION")
        logger.info("=" * 80)
        spark = create_spark_session()
        
        # STAGE 2: Process Each Dataset
        logger.info(f"\n📋 Processing {len(DENIAL_CONSTRAINTS)} datasets...\n")
        
        pipeline_summary = {}
        
        for dataset_key, dataset_config in DENIAL_CONSTRAINTS.items():
            table_name = dataset_config["table_name"]
            
            try:
                logger.info(f"\n{'#' * 80}")
                logger.info(f"# Processing: {table_name}")
                logger.info(f"{'#' * 80}")
                
                # Step 1: Read from Formatted Zone
                logger.info(f"\n[STEP 1: LOAD]")
                spark_df = read_from_formatted_zone(table_name)
                spark_df = prepare_dataset_for_trusted_zone(spark_df, dataset_key)
                
                # Step 2: Assess data quality
                logger.info(f"\n[STEP 2: ASSESS]")
                violations = assess_data_quality(spark_df, dataset_key)
                
                # Step 3: Clean data
                logger.info(f"\n[STEP 3: CLEAN]")
                cleaned_df = clean_dataset(spark_df, dataset_key)
                
                # Step 4: Save to Trusted Zone
                logger.info(f"\n[STEP 4: SAVE]")
                save_to_trusted_zone(cleaned_df, table_name)
                
                # Record statistics
                pipeline_summary[table_name] = {
                    "initial_count": spark_df.count(),
                    "final_count": cleaned_df.count(),
                    "violations": violations,
                }
                
                logger.info(f"\n✓ Successfully processed '{table_name}'")
            
            except Exception as e:
                logger.error(f"\n✗ Error processing '{table_name}': {str(e)}")
                pipeline_summary[table_name] = {"error": str(e)}
        
        # STAGE 3: Generate Final Report
        logger.info(f"\n\n{'=' * 80}")
        logger.info("TRUSTED ZONE PIPELINE - FINAL SUMMARY REPORT")
        logger.info(f"{'=' * 80}")
        
        total_initial = 0
        total_final = 0
        total_removed = 0
        
        for table_name, stats in pipeline_summary.items():
            if "error" not in stats:
                initial = stats["initial_count"]
                final = stats["final_count"]
                removed = initial - final
                
                total_initial += initial
                total_final += final
                total_removed += removed
                
                logger.info(f"\n{table_name}:")
                logger.info(f"  Initial records: {initial:,}")
                logger.info(f"  Final records: {final:,}")
                logger.info(f"  Records removed: {removed:,}")
                logger.info(f"  Quality score: {(final / initial * 100):.2f}%")
            else:
                logger.error(f"\n{table_name}: ERROR - {stats['error']}")
        
        logger.info(f"\n{'-' * 80}")
        logger.info(f"GLOBAL STATISTICS:")
        logger.info(f"  Total initial records (across all datasets): {total_initial:,}")
        logger.info(f"  Total final records (cleaned): {total_final:,}")
        logger.info(f"  Total records removed: {total_removed:,}")
        if total_initial > 0:
            logger.info(f"  Overall data quality: {(total_final / total_initial * 100):.2f}%")
        logger.info(f"{'=' * 80}")
        
        logger.info(f"\n✓ Pipeline completed successfully!")
        logger.info(f"✓ Cleaned data saved to: {TRUSTED_DB_PATH}")
        logger.info(f"  Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    except Exception as e:
        logger.error(f"\n✗ Pipeline execution failed: {str(e)}", exc_info=True)
        raise
    
    finally:

        # STAGE 4: Cleanup Resources (extra step to ensure Spark session is closed even if errors occur)
        if spark is not None:
            try:
                logger.info(f"\nClosing Spark session...")
                spark.stop()
                logger.info(f"Spark session closed successfully")
            except Exception as e:
                logger.error(f"Error closing Spark session: {str(e)}")


if __name__ == "__main__":
    """
    Main execution point of the Trusted Zone Pipeline.
    """
    import os
    try:
        execute_trusted_zone_pipeline()
        os._exit(0) 
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}")
        os._exit(1) 
