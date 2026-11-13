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

df_tests = spark.createDataFrame(
    test_data,
    schema=test_schema
)

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
            F.array_contains(
                F.col("detected_rules"),
                F.col("expected_rule")
            ),
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
# 5. Validate and display the test results
# ============================================================

total_tests = test_results.count()

passed_tests = (
    test_results
    .filter(F.col("test_status") == "PASSED")
    .count()
)

failed_tests = (
    test_results
    .filter(F.col("test_status") == "FAILED")
    .count()
)

duplicate_test_count = (
    test_results
    .groupBy("test_name")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

null_expected_rule_count = (
    test_results
    .filter(F.col("expected_rule").isNull())
    .count()
)

empty_detected_rules_count = (
    test_results
    .filter(F.size(F.col("detected_rules")) == 0)
    .count()
)

test_summary = (
    test_results
    .groupBy("test_status")
    .count()
    .orderBy("test_status")
)

display(test_results)
display(test_summary)

assert total_tests == 9, (
    f"Negative test count validation failed: "
    f"expected 9, found {total_tests}."
)

assert duplicate_test_count == 0, (
    f"Duplicate test validation failed: "
    f"found {duplicate_test_count} duplicate test names."
)

assert null_expected_rule_count == 0, (
    f"Expected rule validation failed: "
    f"found {null_expected_rule_count} null values."
)

assert empty_detected_rules_count == 0, (
    f"Detected rule validation failed: "
    f"found {empty_detected_rules_count} empty rule arrays."
)

assert failed_tests == 0, (
    f"Negative tests failed: "
    f"{failed_tests} of {total_tests}."
)

assert passed_tests == total_tests, (
    f"Negative test summary validation failed: "
    f"expected {total_tests} passed tests, found {passed_tests}."
)

print(
    f"All negative tests PASSED: "
    f"{passed_tests} of {total_tests}."
)

# ============================================================
# 6. Save SQL-compatible negative test results
# ============================================================

target_table = "`04_monitoring`.negative_test_results"

negative_test_results = (
    test_results
    .withColumn(
        "detected_rules",
        F.to_json(F.col("detected_rules"))
    )
    .withColumn(
        "test_run_at",
        F.current_timestamp()
    )
    .withColumn(
        "source_period",
        F.lit("2026-01")
    )
)

detected_rules_type = (
    negative_test_results
    .schema["detected_rules"]
    .dataType
)

assert isinstance(detected_rules_type, StringType), (
    "Pre-write schema validation failed: "
    "detected_rules must be STRING."
)

assert negative_test_results.count() == total_tests, (
    "Pre-write row count validation failed."
)

negative_test_results.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(target_table)

# ============================================================
# 7. Validate the persisted negative test results
# ============================================================

saved_results = spark.table(target_table)

saved_row_count = saved_results.count()

saved_detected_rules_type = (
    saved_results
    .schema["detected_rules"]
    .dataType
)

null_detected_rules_count = (
    saved_results
    .filter(F.col("detected_rules").isNull())
    .count()
)

invalid_json_count = (
    saved_results
    .filter(
        F.from_json(
            F.col("detected_rules"),
            "array<string>"
        ).isNull()
    )
    .count()
)

failed_saved_test_count = (
    saved_results
    .filter(F.col("test_status") != "PASSED")
    .count()
)

duplicate_saved_test_count = (
    saved_results
    .groupBy("test_name")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

assert saved_row_count == total_tests, (
    f"Post-write row count validation failed: "
    f"expected {total_tests}, found {saved_row_count}."
)

assert isinstance(saved_detected_rules_type, StringType), (
    "Post-write schema validation failed: "
    "detected_rules must be STRING."
)

assert null_detected_rules_count == 0, (
    f"Post-write null validation failed: "
    f"found {null_detected_rules_count} null detected_rules values."
)

assert invalid_json_count == 0, (
    f"Post-write JSON validation failed: "
    f"found {invalid_json_count} invalid detected_rules values."
)

assert failed_saved_test_count == 0, (
    f"Post-write test status validation failed: "
    f"found {failed_saved_test_count} failed tests."
)

assert duplicate_saved_test_count == 0, (
    f"Post-write duplicate validation failed: "
    f"found {duplicate_saved_test_count} duplicate test names."
)

display(saved_results.orderBy("test_name"))

print(f"Saved table: {target_table}")
print(f"Saved rows: {saved_row_count}")
print("detected_rules data type: STRING")
print("Negative test results validation PASSED.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

# ============================================================
# 8. Reconcile the Bronze, Silver, and Quarantine layers
# ============================================================

bronze_table = "`01_bronze`.yellow_taxi_trips"
silver_table = "`02_silver`.yellow_taxi_trips"
quarantine_table = "`03_quarantine`.yellow_taxi_trips"
reconciliation_table = "`04_monitoring`.reconciliation_results"

required_tables = [
    bronze_table,
    silver_table,
    quarantine_table
]

missing_tables = [
    table_name
    for table_name in required_tables
    if not spark.catalog.tableExists(table_name)
]

assert len(missing_tables) == 0, (
    f"Required table validation failed. "
    f"Missing tables: {missing_tables}"
)

bronze_count = spark.table(bronze_table).count()
silver_count = spark.table(silver_table).count()
quarantine_count = spark.table(quarantine_table).count()

reconciled_count = silver_count + quarantine_count
count_difference = bronze_count - reconciled_count

reconciliation_status = (
    "PASSED"
    if count_difference == 0
    else "FAILED"
)

reconciliation_result = (
    spark.createDataFrame(
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
    )
    .withColumn(
        "reconciliation_run_at",
        F.current_timestamp()
    )
    .withColumn(
        "source_period",
        F.lit("2026-01")
    )
)

assert bronze_count > 0, (
    "Reconciliation validation failed: "
    "Bronze contains no records."
)

assert silver_count >= 0, (
    "Reconciliation validation failed: "
    "Silver count cannot be negative."
)

assert quarantine_count >= 0, (
    "Reconciliation validation failed: "
    "Quarantine count cannot be negative."
)

assert count_difference == 0, (
    f"Reconciliation failed. "
    f"Difference: {count_difference}."
)

reconciliation_result.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(reconciliation_table)

# ============================================================
# 9. Validate the persisted reconciliation result
# ============================================================

saved_reconciliation = spark.table(reconciliation_table)

saved_reconciliation_count = saved_reconciliation.count()

failed_reconciliation_count = (
    saved_reconciliation
    .filter(F.col("reconciliation_status") != "PASSED")
    .count()
)

nonzero_difference_count = (
    saved_reconciliation
    .filter(F.col("count_difference") != 0)
    .count()
)

assert saved_reconciliation_count == 1, (
    f"Post-write reconciliation row count validation failed: "
    f"expected 1, found {saved_reconciliation_count}."
)

assert failed_reconciliation_count == 0, (
    f"Post-write reconciliation status validation failed: "
    f"found {failed_reconciliation_count} failed rows."
)

assert nonzero_difference_count == 0, (
    f"Post-write reconciliation difference validation failed: "
    f"found {nonzero_difference_count} rows with a nonzero difference."
)

display(saved_reconciliation)

print("Reconciliation PASSED.")
print(f"Bronze records: {bronze_count}")
print(f"Silver records: {silver_count}")
print(f"Quarantine records: {quarantine_count}")
print(f"Count difference: {count_difference}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

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
