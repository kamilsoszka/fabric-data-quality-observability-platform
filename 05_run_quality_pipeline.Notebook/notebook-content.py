# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "e3c81730-469e-4ca0-a08f-bf941ac02c6f",
# META       "default_lakehouse_name": "lh_data_quality",
# META       "default_lakehouse_workspace_id": "4059fec2-4e4c-4b6d-b7f8-273305c44bed",
# META       "known_lakehouses": [
# META         {
# META           "id": "e3c81730-469e-4ca0-a08f-bf941ac02c6f"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from datetime import datetime, timezone
from uuid import uuid4

from pyspark.sql import functions as F

# Define pipeline configuration
source_table = "`01_bronze`.yellow_taxi_trips"
silver_table = "`02_silver`.yellow_taxi_trips"
quarantine_table = "`03_quarantine`.yellow_taxi_trips"
summary_table = "`04_monitoring`.pipeline_run_history"
rule_results_table = "`04_monitoring`.quality_rule_history"

source_period = "2026-01"
run_id = str(uuid4())
run_started_at = datetime.now(timezone.utc).replace(tzinfo=None)

print(f"Pipeline run ID: {run_id}")
print(f"Pipeline started at: {run_started_at}")

# Read the Bronze source table
df_bronze = spark.table(source_table)

bronze_count = df_bronze.count()

# Define production quality rules
quality_rules = F.array(
    F.when(
        F.col("VendorID").isNull(),
        F.lit("VENDOR_ID_NULL")
    ),
    F.when(
        F.col("tpep_pickup_datetime").isNull(),
        F.lit("PICKUP_DATETIME_NULL")
    ),
    F.when(
        F.col("tpep_dropoff_datetime").isNull(),
        F.lit("DROPOFF_DATETIME_NULL")
    ),
    F.when(
        F.col("tpep_dropoff_datetime")
        < F.col("tpep_pickup_datetime"),
        F.lit("DROPOFF_BEFORE_PICKUP")
    ),
    F.when(
        F.col("trip_distance") < 0,
        F.lit("NEGATIVE_TRIP_DISTANCE")
    ),
    F.when(
        F.col("PULocationID").isNull(),
        F.lit("PICKUP_LOCATION_NULL")
    ),
    F.when(
        F.col("DOLocationID").isNull(),
        F.lit("DROPOFF_LOCATION_NULL")
    ),
    F.when(
        F.col("fare_amount").isNull(),
        F.lit("FARE_AMOUNT_NULL")
    ),
    F.when(
        F.col("total_amount").isNull(),
        F.lit("TOTAL_AMOUNT_NULL")
    )
)

# Apply all quality rules
df_validated = (
    df_bronze
    .withColumn(
        "_failed_rules",
        F.filter(
            quality_rules,
            lambda rule: rule.isNotNull()
        )
    )
    .withColumn(
        "_quality_status",
        F.when(
            F.size("_failed_rules") == 0,
            F.lit("PASSED")
        ).otherwise(F.lit("QUARANTINED"))
    )
    .withColumn(
        "_quality_run_id",
        F.lit(run_id)
    )
    .withColumn(
        "_quality_checked_at",
        F.current_timestamp()
    )
)

# Split valid and invalid records
df_silver = (
    df_validated
    .filter(F.col("_quality_status") == "PASSED")
)

df_quarantine = (
    df_validated
    .filter(F.col("_quality_status") == "QUARANTINED")
)

# Persist Silver and Quarantine tables
df_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(silver_table)

df_quarantine.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(quarantine_table)

# Count persisted records
silver_count = spark.table(silver_table).count()
quarantine_count = spark.table(quarantine_table).count()

reconciled_count = silver_count + quarantine_count
count_difference = bronze_count - reconciled_count

reconciliation_status = (
    "PASSED"
    if count_difference == 0
    else "FAILED"
)

# Create rule-level monitoring results
rule_results = (
    df_validated
    .select(
        F.explode_outer("_failed_rules").alias("rule_name")
    )
    .filter(F.col("rule_name").isNotNull())
    .groupBy("rule_name")
    .count()
    .withColumnRenamed("count", "failed_records")
    .withColumn("run_id", F.lit(run_id))
    .withColumn("source_period", F.lit(source_period))
    .withColumn("total_records", F.lit(bronze_count))
    .withColumn(
        "failure_percentage",
        F.round(
            F.col("failed_records")
            / F.col("total_records")
            * 100,
            6
        )
    )
    .withColumn("recorded_at", F.current_timestamp())
    .select(
        "run_id",
        "source_period",
        "rule_name",
        "failed_records",
        "total_records",
        "failure_percentage",
        "recorded_at"
    )
)

# Append rule results to monitoring history
rule_results.write \
    .format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .saveAsTable(rule_results_table)

run_completed_at = datetime.now(timezone.utc).replace(tzinfo=None)
duration_seconds = (
    run_completed_at - run_started_at
).total_seconds()

pipeline_status = (
    "PASSED"
    if count_difference == 0
    else "FAILED"
)

# Create the pipeline run summary
run_summary = spark.createDataFrame(
    [(
        run_id,
        source_period,
        run_started_at,
        run_completed_at,
        duration_seconds,
        bronze_count,
        silver_count,
        quarantine_count,
        reconciled_count,
        count_difference,
        reconciliation_status,
        pipeline_status
    )],
    [
        "run_id",
        "source_period",
        "run_started_at",
        "run_completed_at",
        "duration_seconds",
        "bronze_count",
        "silver_count",
        "quarantine_count",
        "reconciled_count",
        "count_difference",
        "reconciliation_status",
        "pipeline_status"
    ]
)

# Append the run summary to monitoring history
run_summary.write \
    .format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .saveAsTable(summary_table)

display(run_summary)
display(rule_results)

assert count_difference == 0, (
    f"Pipeline reconciliation failed. "
    f"Difference: {count_difference}"
)

print("Quality pipeline PASSED.")
print(f"Bronze records: {bronze_count:,}")
print(f"Silver records: {silver_count:,}")
print(f"Quarantine records: {quarantine_count:,}")
print(f"Count difference: {count_difference:,}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
