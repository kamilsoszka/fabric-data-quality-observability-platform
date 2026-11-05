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

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType
)

# Define the source table
source_table = "`01_bronze`.yellow_taxi_trips"

# Define the expected schema contract
expected_schema = [
    ("VendorID", "int"),
    ("tpep_pickup_datetime", "timestamp_ntz"),
    ("tpep_dropoff_datetime", "timestamp_ntz"),
    ("passenger_count", "bigint"),
    ("trip_distance", "double"),
    ("RatecodeID", "bigint"),
    ("store_and_fwd_flag", "string"),
    ("PULocationID", "int"),
    ("DOLocationID", "int"),
    ("payment_type", "bigint"),
    ("fare_amount", "double"),
    ("extra", "double"),
    ("mta_tax", "double"),
    ("tip_amount", "double"),
    ("tolls_amount", "double"),
    ("improvement_surcharge", "double"),
    ("total_amount", "double"),
    ("congestion_surcharge", "double"),
    ("Airport_fee", "double"),
    ("cbd_congestion_fee", "double"),
    ("_ingested_at", "timestamp"),
    ("_source_file", "string"),
    ("_source_period", "string")
]

# Read the actual table schema
actual_struct = spark.table(source_table).schema

actual_schema = [
    (field.name, field.dataType.simpleString())
    for field in actual_struct.fields
]

# Compare expected and actual schemas
maximum_length = max(
    len(expected_schema),
    len(actual_schema)
)

validation_rows = []

for position in range(maximum_length):
    expected_column = (
        expected_schema[position]
        if position < len(expected_schema)
        else (None, None)
    )

    actual_column = (
        actual_schema[position]
        if position < len(actual_schema)
        else (None, None)
    )

    expected_name, expected_type = expected_column
    actual_name, actual_type = actual_column

    if expected_name is None:
        validation_status = "UNEXPECTED_COLUMN"
    elif actual_name is None:
        validation_status = "MISSING_COLUMN"
    elif expected_name != actual_name:
        validation_status = "COLUMN_NAME_OR_ORDER_MISMATCH"
    elif expected_type != actual_type:
        validation_status = "DATA_TYPE_MISMATCH"
    else:
        validation_status = "PASSED"

    validation_rows.append((
        position + 1,
        expected_name,
        expected_type,
        actual_name,
        actual_type,
        validation_status
    ))

# Define the validation result schema
result_schema = StructType([
    StructField("ordinal_position", StringType(), False),
    StructField("expected_column_name", StringType(), True),
    StructField("expected_data_type", StringType(), True),
    StructField("actual_column_name", StringType(), True),
    StructField("actual_data_type", StringType(), True),
    StructField("validation_status", StringType(), False)
])

# Convert ordinal positions to strings for stable schema creation
validation_rows_as_strings = [
    (
        str(row[0]),
        row[1],
        row[2],
        row[3],
        row[4],
        row[5]
    )
    for row in validation_rows
]

schema_validation_results = (
    spark.createDataFrame(
        validation_rows_as_strings,
        schema=result_schema
    )
    .withColumn(
        "validation_run_at",
        F.current_timestamp()
    )
    .withColumn(
        "source_table",
        F.lit("01_bronze.yellow_taxi_trips")
    )
    .withColumn(
        "source_period",
        F.lit("2026-01")
    )
)

# Count schema mismatches
failed_columns = (
    schema_validation_results
    .filter(F.col("validation_status") != "PASSED")
    .count()
)

total_columns = schema_validation_results.count()
passed_columns = total_columns - failed_columns

# Save the validation results
schema_validation_results.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(
        "`04_monitoring`.schema_validation_results"
    )

display(schema_validation_results)

assert failed_columns == 0, (
    f"Schema validation failed: "
    f"{failed_columns} mismatches detected."
)

print(
    f"Schema validation PASSED: "
    f"{passed_columns} of {total_columns} columns."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
