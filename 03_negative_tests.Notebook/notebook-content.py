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

from datetime import datetime

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    TimestampType
)

# ============================================================
# 1. Define the test data schema
# ============================================================

test_schema = StructType([
    StructField("test_name", StringType(), False),
    StructField("VendorID", IntegerType(), True),
    StructField("tpep_pickup_datetime", TimestampType(), True),
    StructField("tpep_dropoff_datetime", TimestampType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("PULocationID", IntegerType(), True),
    StructField("DOLocationID", IntegerType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("expected_rule", StringType(), False)
])

pickup = datetime(2026, 1, 10, 12, 0, 0)
dropoff = datetime(2026, 1, 10, 13, 0, 0)

# ============================================================
# 2. Create one invalid record for each quality rule
# ============================================================

test_data = [
    (
        "vendor_null",
        None,
        pickup,
        dropoff,
        5.0,
        100,
        200,
        20.0,
        25.0,
        "VENDOR_ID_NULL"
    ),
    (
        "pickup_datetime_null",
        1,
        None,
        dropoff,
        5.0,
        100,
        200,
        20.0,
        25.0,
        "PICKUP_DATETIME_NULL"
    ),
    (
        "dropoff_datetime_null",
        1,
        pickup,
        None,
        5.0,
        100,
        200,
        20.0,
        25.0,
        "DROPOFF_DATETIME_NULL"
    ),
    (
        "dropoff_before_pickup",
        1,
        pickup,
        datetime(2026, 1, 10, 11, 0, 0),
        5.0,
        100,
        200,
        20.0,
        25.0,
        "DROPOFF_BEFORE_PICKUP"
    ),
    (
        "negative_trip_distance",
        1,
        pickup,
        dropoff,
        -1.0,
        100,
        200,
        20.0,
        25.0,
        "NEGATIVE_TRIP_DISTANCE"
    ),
    (
        "pickup_location_null",
        1,
        pickup,
        dropoff,
        5.0,
        None,
        200,
        20.0,
        25.0,
        "PICKUP_LOCATION_NULL"
    ),
    (
        "dropoff_location_null",
        1,
        pickup,
        dropoff,
        5.0,
        100,
        None,
        20.0,
        25.0,
        "DROPOFF_LOCATION_NULL"
    ),
    (
        "fare_amount_null",
        1,
        pickup,
        dropoff,
        5.0,
        100,
        200,
        None,
        25.0,
        "FARE_AMOUNT_NULL"
    ),
    (
        "total_amount_null",
        1,
        pickup,
        dropoff,
        5.0,
        100,
        200,
        20.0,
        None,
        "TOTAL_AMOUNT_NULL"
    )
]

df_tests = spark.createDataFrame(test_data, schema=test_schema)

# ============================================================
# 3. Apply the production quality rules
# ============================================================

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

# ============================================================
# 4. Execute the tests and compare detected rules
# ============================================================

test_results = (
    df_tests
    .withColumn(
        "detected_rules",
        F.filter(
            quality_rules,
            lambda rule: rule.isNotNull()
        )
    )
    .withColumn(
        "test_status",
        F.when(
            F.expr("array_contains(detected_rules, expected_rule)"),
            F.lit("PASSED")
        ).otherwise(F.lit("FAILED"))
    )
    .select(
        "test_name",
        "expected_rule",
        "detected_rules",
        "test_status"
    )
    .orderBy("test_name")
)

# ============================================================
# 5. Create the test summary
# ============================================================

test_summary = (
    test_results
    .groupBy("test_status")
    .count()
    .orderBy("test_status")
)

failed_tests = (
    test_results
    .filter(F.col("test_status") == "FAILED")
    .count()
)

total_tests = test_results.count()
passed_tests = total_tests - failed_tests

display(test_results)
display(test_summary)

# ============================================================
# 6. Fail the notebook if any test fails
# ============================================================

assert failed_tests == 0, (
    f"Negative tests failed: {failed_tests} of {total_tests}"
)

print(f"All negative tests PASSED: {passed_tests} of {total_tests}.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

negative_test_results = (
    test_results
    .withColumn("test_run_at", F.current_timestamp())
    .withColumn("source_period", F.lit("2026-01"))
)

negative_test_results.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("`04_monitoring`.negative_test_results")

print("Saved: 04_monitoring.negative_test_results")
print(f"Saved rows: {negative_test_results.count()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

# Count records in each data layer
bronze_count = spark.table(
    "`01_bronze`.yellow_taxi_trips"
).count()

silver_count = spark.table(
    "`02_silver`.yellow_taxi_trips"
).count()

quarantine_count = spark.table(
    "`03_quarantine`.yellow_taxi_trips"
).count()

reconciled_count = silver_count + quarantine_count
count_difference = bronze_count - reconciled_count

reconciliation_status = (
    "PASSED"
    if count_difference == 0
    else "FAILED"
)

# Create the reconciliation result
reconciliation_result = spark.createDataFrame(
    [(
        "yellow_taxi_trips",
        bronze_count,
        silver_count,
        quarantine_count,
        reconciled_count,
        count_difference,
        reconciliation_status
    )],
    [
        "dataset_name",
        "bronze_count",
        "silver_count",
        "quarantine_count",
        "reconciled_count",
        "count_difference",
        "reconciliation_status"
    ]
).withColumn(
    "reconciliation_run_at",
    F.current_timestamp()
).withColumn(
    "source_period",
    F.lit("2026-01")
)

# Save the reconciliation result
reconciliation_result.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(
        "`04_monitoring`.reconciliation_results"
    )

display(reconciliation_result)

assert count_difference == 0, (
    f"Reconciliation failed. Difference: {count_difference}"
)

print("Reconciliation PASSED.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
