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

import json
from datetime import datetime, timezone
from uuid import uuid4

from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.storagelevel import StorageLevel


# Define pipeline configuration
source_table = "`01_bronze`.yellow_taxi_trips"
silver_table = "`02_silver`.yellow_taxi_trips"
quarantine_table = "`03_quarantine`.yellow_taxi_trips"

quality_run_summary_table = "`04_monitoring`.quality_run_summary"
quality_rule_results_table = "`04_monitoring`.quality_rule_results"
freshness_sla_results_table = "`04_monitoring`.freshness_sla_results"
reconciliation_results_table = "`04_monitoring`.reconciliation_results"
pipeline_run_history_table = "`04_monitoring`.pipeline_run_history"
quality_rule_history_table = "`04_monitoring`.quality_rule_history"

source_table_name = "01_bronze.yellow_taxi_trips"
dataset_name = "yellow_taxi_trips"
source_period = "2026-01"
freshness_sla_threshold_hours = 24

expected_rule_names = [
    "VENDOR_ID_NULL",
    "PICKUP_DATETIME_NULL",
    "DROPOFF_DATETIME_NULL",
    "DROPOFF_BEFORE_PICKUP",
    "NEGATIVE_TRIP_DISTANCE",
    "PICKUP_LOCATION_NULL",
    "DROPOFF_LOCATION_NULL",
    "FARE_AMOUNT_NULL",
    "TOTAL_AMOUNT_NULL",
]

required_source_columns = [
    "VendorID",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "trip_distance",
    "PULocationID",
    "DOLocationID",
    "fare_amount",
    "total_amount",
    "_ingested_at",
]

run_id = str(uuid4())
run_started_at = datetime.now(
    timezone.utc
).replace(tzinfo=None)

quality_checked_at = run_started_at


def require_table(table_name):
    if not spark.catalog.tableExists(table_name):
        raise RuntimeError(
            f"Required table does not exist: {table_name}"
        )


def require_columns(
    dataframe,
    required_columns,
    dataframe_name,
):
    missing_columns = sorted(
        set(required_columns) - set(dataframe.columns)
    )

    if missing_columns:
        raise RuntimeError(
            f"{dataframe_name} is missing required columns: "
            f"{missing_columns}"
        )


def create_pipeline_history(
    pipeline_status,
    bronze_count,
    silver_count,
    quarantine_count,
    reconciled_count,
    count_difference,
    reconciliation_status,
):
    run_completed_at = datetime.now(
        timezone.utc
    ).replace(tzinfo=None)

    duration_seconds = (
        run_completed_at - run_started_at
    ).total_seconds()

    schema = T.StructType([
        T.StructField("run_id", T.StringType(), False),
        T.StructField(
            "source_period",
            T.StringType(),
            False,
        ),
        T.StructField(
            "run_started_at",
            T.TimestampType(),
            False,
        ),
        T.StructField(
            "run_completed_at",
            T.TimestampType(),
            False,
        ),
        T.StructField(
            "duration_seconds",
            T.DoubleType(),
            False,
        ),
        T.StructField(
            "bronze_count",
            T.LongType(),
            False,
        ),
        T.StructField(
            "silver_count",
            T.LongType(),
            False,
        ),
        T.StructField(
            "quarantine_count",
            T.LongType(),
            False,
        ),
        T.StructField(
            "reconciled_count",
            T.LongType(),
            False,
        ),
        T.StructField(
            "count_difference",
            T.LongType(),
            False,
        ),
        T.StructField(
            "reconciliation_status",
            T.StringType(),
            False,
        ),
        T.StructField(
            "pipeline_status",
            T.StringType(),
            False,
        ),
    ])

    return spark.createDataFrame(
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
            pipeline_status,
        )],
        schema=schema,
    )


print(f"Pipeline run ID: {run_id}")
print(f"Pipeline started at: {run_started_at}")

require_table(source_table)

df_bronze = spark.table(source_table)

require_columns(
    df_bronze,
    required_source_columns,
    "Bronze source",
)

bronze_count = df_bronze.count()

if bronze_count <= 0:
    raise RuntimeError(
        "Bronze source contains no records."
    )


# Calculate freshness before changing Silver or Quarantine
freshness_source = (
    df_bronze
    .agg(
        F.max("_ingested_at").alias(
            "last_ingested_at"
        )
    )
    .first()
)

last_ingested_at = freshness_source[
    "last_ingested_at"
]

if last_ingested_at is None:
    raise RuntimeError(
        "Freshness validation failed because "
        "_ingested_at contains no non-null values."
    )

age_hours = (
    quality_checked_at - last_ingested_at
).total_seconds() / 3600.0

if age_hours < 0:
    raise RuntimeError(
        "Freshness age cannot be negative. "
        f"Calculated age: {age_hours}."
    )

sla_status = (
    "PASSED"
    if age_hours <= freshness_sla_threshold_hours
    else "FAILED"
)

freshness_schema = T.StructType([
    T.StructField(
        "last_ingested_at",
        T.TimestampType(),
        False,
    ),
    T.StructField(
        "record_count",
        T.LongType(),
        False,
    ),
    T.StructField(
        "checked_at",
        T.TimestampType(),
        False,
    ),
    T.StructField(
        "age_hours",
        T.DoubleType(),
        False,
    ),
    T.StructField(
        "sla_threshold_hours",
        T.IntegerType(),
        False,
    ),
    T.StructField(
        "sla_status",
        T.StringType(),
        False,
    ),
    T.StructField(
        "source_period",
        T.StringType(),
        False,
    ),
])

freshness_results = spark.createDataFrame(
    [(
        last_ingested_at,
        bronze_count,
        quality_checked_at,
        round(age_hours, 2),
        freshness_sla_threshold_hours,
        sla_status,
        source_period,
    )],
    schema=freshness_schema,
)

freshness_results.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(freshness_sla_results_table)

persisted_freshness = spark.table(
    freshness_sla_results_table
)

if persisted_freshness.count() != 1:
    raise RuntimeError(
        "freshness_sla_results must contain "
        "exactly one row."
    )

persisted_freshness_row = (
    persisted_freshness.first()
)

if (
    persisted_freshness_row["record_count"]
    != bronze_count
):
    raise RuntimeError(
        "Freshness record count does not match Bronze."
    )


# Stop safely before changing downstream tables
if sla_status != "PASSED":
    failed_pipeline_history = create_pipeline_history(
        pipeline_status="FAILED",
        bronze_count=bronze_count,
        silver_count=0,
        quarantine_count=0,
        reconciled_count=0,
        count_difference=bronze_count,
        reconciliation_status="NOT_EXECUTED",
    )

    failed_pipeline_history.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable(pipeline_run_history_table)

    failure_payload = {
        "run_id": run_id,
        "source_period": source_period,
        "pipeline_status": "FAILED",
        "failure_reason": "FRESHNESS_SLA_FAILED",
        "freshness_sla_status": sla_status,
        "age_hours": round(age_hours, 2),
        "sla_threshold_hours": (
            freshness_sla_threshold_hours
        ),
        "bronze_count": bronze_count,
    }

    print(json.dumps(failure_payload, indent=2))

    raise RuntimeError(
        "Freshness SLA failed before downstream "
        "tables were changed. "
        f"Age: {age_hours:.2f} hours. "
        f"Threshold: "
        f"{freshness_sla_threshold_hours} hours."
    )


# Define production quality rules
quality_rules = F.array(
    F.when(
        F.col("VendorID").isNull(),
        F.lit("VENDOR_ID_NULL"),
    ),
    F.when(
        F.col("tpep_pickup_datetime").isNull(),
        F.lit("PICKUP_DATETIME_NULL"),
    ),
    F.when(
        F.col("tpep_dropoff_datetime").isNull(),
        F.lit("DROPOFF_DATETIME_NULL"),
    ),
    F.when(
        F.col("tpep_dropoff_datetime")
        < F.col("tpep_pickup_datetime"),
        F.lit("DROPOFF_BEFORE_PICKUP"),
    ),
    F.when(
        F.col("trip_distance") < 0,
        F.lit("NEGATIVE_TRIP_DISTANCE"),
    ),
    F.when(
        F.col("PULocationID").isNull(),
        F.lit("PICKUP_LOCATION_NULL"),
    ),
    F.when(
        F.col("DOLocationID").isNull(),
        F.lit("DROPOFF_LOCATION_NULL"),
    ),
    F.when(
        F.col("fare_amount").isNull(),
        F.lit("FARE_AMOUNT_NULL"),
    ),
    F.when(
        F.col("total_amount").isNull(),
        F.lit("TOTAL_AMOUNT_NULL"),
    ),
)

df_validated = (
    df_bronze
    .withColumn(
        "_failed_rules",
        F.filter(
            quality_rules,
            lambda rule: rule.isNotNull(),
        ),
    )
    .withColumn(
        "_quality_status",
        F.when(
            F.size("_failed_rules") == 0,
            F.lit("PASSED"),
        ).otherwise(F.lit("QUARANTINED")),
    )
    .withColumn(
        "_quality_run_id",
        F.lit(run_id),
    )
    .withColumn(
        "_quality_checked_at",
        F.lit(quality_checked_at).cast(
            "timestamp"
        ),
    )
    .persist(StorageLevel.MEMORY_AND_DISK)
)

validated_count = df_validated.count()

if validated_count != bronze_count:
    raise RuntimeError(
        "Validated count does not match Bronze."
    )

df_silver = df_validated.filter(
    F.col("_quality_status") == "PASSED"
)

df_quarantine = df_validated.filter(
    F.col("_quality_status") == "QUARANTINED"
)

silver_prewrite_count = df_silver.count()
quarantine_prewrite_count = df_quarantine.count()

if (
    silver_prewrite_count
    + quarantine_prewrite_count
    != bronze_count
):
    raise RuntimeError(
        "Pre-write reconciliation failed."
    )


# Create complete rule results
rule_catalog_schema = T.StructType([
    T.StructField(
        "rule_name",
        T.StringType(),
        False,
    ),
])

rule_catalog = spark.createDataFrame(
    [(name,) for name in expected_rule_names],
    schema=rule_catalog_schema,
)

failed_rule_counts = (
    df_validated
    .select(
        F.explode("_failed_rules").alias(
            "rule_name"
        )
    )
    .groupBy("rule_name")
    .agg(
        F.count(F.lit(1))
        .cast("long")
        .alias("failed_records")
    )
)

rule_results_history = (
    rule_catalog
    .join(
        failed_rule_counts,
        on="rule_name",
        how="left",
    )
    .withColumn(
        "failed_records",
        F.coalesce(
            F.col("failed_records"),
            F.lit(0).cast("long"),
        ),
    )
    .withColumn("run_id", F.lit(run_id))
    .withColumn(
        "source_period",
        F.lit(source_period),
    )
    .withColumn(
        "total_records",
        F.lit(bronze_count).cast("int"),
    )
    .withColumn(
        "failure_percentage",
        F.round(
            F.col("failed_records").cast("double")
            / F.col("total_records").cast("double")
            * F.lit(100.0),
            6,
        ),
    )
    .withColumn(
        "recorded_at",
        F.lit(quality_checked_at).cast(
            "timestamp"
        ),
    )
    .select(
        "run_id",
        "source_period",
        "rule_name",
        "failed_records",
        "total_records",
        "failure_percentage",
        "recorded_at",
    )
)

rule_result_check = (
    rule_results_history
    .agg(
        F.count("*").alias("row_count"),
        F.countDistinct("rule_name").alias(
            "distinct_rule_count"
        ),
    )
    .first()
)

if (
    rule_result_check["row_count"] != 9
    or rule_result_check[
        "distinct_rule_count"
    ] != 9
):
    raise RuntimeError(
        "Rule results must contain exactly nine "
        "unique rules."
    )

rule_results_snapshot = (
    rule_results_history
    .select(
        "rule_name",
        "failed_records",
        "total_records",
        "failure_percentage",
        "source_period",
        F.col("recorded_at").alias(
            "quality_run_at"
        ),
    )
)


# Persist downstream data only after freshness passed
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

silver_count = spark.table(silver_table).count()
quarantine_count = spark.table(
    quarantine_table
).count()

reconciled_count = (
    silver_count + quarantine_count
)

count_difference = (
    bronze_count - reconciled_count
)

reconciliation_status = (
    "PASSED"
    if count_difference == 0
    else "FAILED"
)

if silver_count != silver_prewrite_count:
    raise RuntimeError(
        "Persisted Silver count changed."
    )

if quarantine_count != quarantine_prewrite_count:
    raise RuntimeError(
        "Persisted Quarantine count changed."
    )

if count_difference != 0:
    raise RuntimeError(
        "Post-write reconciliation failed."
    )


# Create monitoring snapshots
quality_summary_schema = T.StructType([
    T.StructField(
        "_quality_status",
        T.StringType(),
        False,
    ),
    T.StructField("count", T.LongType(), False),
    T.StructField(
        "source_table",
        T.StringType(),
        False,
    ),
    T.StructField(
        "quality_run_at",
        T.TimestampType(),
        False,
    ),
    T.StructField(
        "source_period",
        T.StringType(),
        False,
    ),
])

quality_run_summary = spark.createDataFrame(
    [
        (
            "PASSED",
            silver_count,
            source_table_name,
            quality_checked_at,
            source_period,
        ),
        (
            "QUARANTINED",
            quarantine_count,
            source_table_name,
            quality_checked_at,
            source_period,
        ),
    ],
    schema=quality_summary_schema,
)

reconciliation_schema = T.StructType([
    T.StructField(
        "dataset_name",
        T.StringType(),
        False,
    ),
    T.StructField(
        "bronze_count",
        T.LongType(),
        False,
    ),
    T.StructField(
        "silver_count",
        T.LongType(),
        False,
    ),
    T.StructField(
        "quarantine_count",
        T.LongType(),
        False,
    ),
    T.StructField(
        "reconciled_count",
        T.LongType(),
        False,
    ),
    T.StructField(
        "count_difference",
        T.LongType(),
        False,
    ),
    T.StructField(
        "reconciliation_status",
        T.StringType(),
        False,
    ),
    T.StructField(
        "reconciliation_run_at",
        T.TimestampType(),
        False,
    ),
    T.StructField(
        "source_period",
        T.StringType(),
        False,
    ),
])

reconciliation_results = spark.createDataFrame(
    [(
        dataset_name,
        bronze_count,
        silver_count,
        quarantine_count,
        reconciled_count,
        count_difference,
        reconciliation_status,
        quality_checked_at,
        source_period,
    )],
    schema=reconciliation_schema,
)


# Write current snapshots
rule_results_snapshot.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(quality_rule_results_table)

quality_run_summary.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(quality_run_summary_table)

reconciliation_results.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(reconciliation_results_table)


# Validate current snapshots
persisted_rule_results = spark.table(
    quality_rule_results_table
)

if persisted_rule_results.count() != 9:
    raise RuntimeError(
        "quality_rule_results does not contain "
        "exactly nine rows."
    )

if (
    persisted_rule_results
    .select("rule_name")
    .distinct()
    .count()
    != 9
):
    raise RuntimeError(
        "quality_rule_results does not contain "
        "nine unique rules."
    )

if spark.table(
    quality_run_summary_table
).count() != 2:
    raise RuntimeError(
        "quality_run_summary does not contain "
        "exactly two rows."
    )

persisted_reconciliation = spark.table(
    reconciliation_results_table
).first()

if (
    persisted_reconciliation[
        "count_difference"
    ] != 0
):
    raise RuntimeError(
        "Persisted reconciliation failed."
    )


# Append complete rule history
rule_results_history.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable(quality_rule_history_table)

history_for_run = (
    spark.table(quality_rule_history_table)
    .filter(F.col("run_id") == run_id)
)

if history_for_run.count() != 9:
    raise RuntimeError(
        "Current run history does not contain "
        "exactly nine rows."
    )

if (
    history_for_run
    .select("rule_name")
    .distinct()
    .count()
    != 9
):
    raise RuntimeError(
        "Current run history does not contain "
        "nine unique rules."
    )


# Append successful pipeline history
pipeline_run_history = create_pipeline_history(
    pipeline_status="PASSED",
    bronze_count=bronze_count,
    silver_count=silver_count,
    quarantine_count=quarantine_count,
    reconciled_count=reconciled_count,
    count_difference=count_difference,
    reconciliation_status=(
        reconciliation_status
    ),
)

pipeline_run_history.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable(pipeline_run_history_table)

persisted_pipeline_run = (
    spark.table(pipeline_run_history_table)
    .filter(F.col("run_id") == run_id)
)

if persisted_pipeline_run.count() != 1:
    raise RuntimeError(
        "pipeline_run_history must contain exactly "
        "one record for the current run."
    )

df_validated.unpersist()

completed_run = pipeline_run_history.first()

exit_payload = {
    "run_id": run_id,
    "source_period": source_period,
    "pipeline_status": "PASSED",
    "freshness_sla_status": sla_status,
    "freshness_age_hours": round(
        age_hours,
        2,
    ),
    "bronze_count": bronze_count,
    "silver_count": silver_count,
    "quarantine_count": quarantine_count,
    "reconciled_count": reconciled_count,
    "count_difference": count_difference,
    "reconciliation_status": (
        reconciliation_status
    ),
    "rule_result_count": 9,
    "distinct_rule_count": 9,
    "failed_rule_count": (
        persisted_rule_results
        .filter(F.col("failed_records") > 0)
        .count()
    ),
    "duration_seconds": completed_run[
        "duration_seconds"
    ],
}

display(pipeline_run_history)
display(rule_results_history.orderBy("rule_name"))
display(quality_run_summary.orderBy(
    "_quality_status"
))
display(freshness_results)
display(reconciliation_results)

print(json.dumps(exit_payload, indent=2))

notebookutils.notebook.exit(
    json.dumps(exit_payload)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
