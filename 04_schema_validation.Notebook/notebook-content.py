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

from pyspark.sql import functions as F
from pyspark.sql import types as T


# Define validation configuration
source_table = "`01_bronze`.yellow_taxi_trips"
source_table_display_name = "01_bronze.yellow_taxi_trips"
validation_results_table = (
    "`04_monitoring`.schema_validation_results"
)
source_period = "2026-01"

expected_schema = [
    ("VendorID", "int"),
    ("tpep_pickup_datetime", "timestamp"),
    ("tpep_dropoff_datetime", "timestamp"),
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
    ("_source_period", "string"),
]


def require_table(table_name):
    if not spark.catalog.tableExists(table_name):
        raise RuntimeError(
            f"Required table does not exist: {table_name}"
        )


def create_validation_rows(
    expected_columns,
    actual_columns,
):
    maximum_length = max(
        len(expected_columns),
        len(actual_columns),
    )

    rows = []

    for position in range(maximum_length):
        expected_column = (
            expected_columns[position]
            if position < len(expected_columns)
            else (None, None)
        )

        actual_column = (
            actual_columns[position]
            if position < len(actual_columns)
            else (None, None)
        )

        expected_name, expected_type = expected_column
        actual_name, actual_type = actual_column

        if expected_name is None:
            validation_status = "UNEXPECTED_COLUMN"
        elif actual_name is None:
            validation_status = "MISSING_COLUMN"
        elif expected_name != actual_name:
            validation_status = (
                "COLUMN_NAME_OR_ORDER_MISMATCH"
            )
        elif expected_type != actual_type:
            validation_status = "DATA_TYPE_MISMATCH"
        else:
            validation_status = "PASSED"

        rows.append(
            (
                position + 1,
                expected_name,
                expected_type,
                actual_name,
                actual_type,
                validation_status,
            )
        )

    return rows


validation_started_at = datetime.now(
    timezone.utc
).replace(tzinfo=None)

print(
    f"Schema validation started at: "
    f"{validation_started_at}"
)
print(f"Source table: {source_table_display_name}")

require_table(source_table)

actual_struct = spark.table(source_table).schema

actual_schema = [
    (
        field.name,
        field.dataType.simpleString(),
    )
    for field in actual_struct.fields
]

validation_rows = create_validation_rows(
    expected_columns=expected_schema,
    actual_columns=actual_schema,
)

result_schema = T.StructType([
    T.StructField(
        "ordinal_position",
        T.IntegerType(),
        False,
    ),
    T.StructField(
        "expected_column_name",
        T.StringType(),
        True,
    ),
    T.StructField(
        "expected_data_type",
        T.StringType(),
        True,
    ),
    T.StructField(
        "actual_column_name",
        T.StringType(),
        True,
    ),
    T.StructField(
        "actual_data_type",
        T.StringType(),
        True,
    ),
    T.StructField(
        "validation_status",
        T.StringType(),
        False,
    ),
])

schema_validation_results = (
    spark.createDataFrame(
        validation_rows,
        schema=result_schema,
    )
    .withColumn(
        "validation_run_at",
        F.lit(validation_started_at).cast(
            T.TimestampType()
        ),
    )
    .withColumn(
        "source_table",
        F.lit(source_table_display_name),
    )
    .withColumn(
        "source_period",
        F.lit(source_period),
    )
)

validation_summary = (
    schema_validation_results
    .agg(
        F.count("*").alias("total_columns"),
        F.sum(
            F.when(
                F.col("validation_status") == "PASSED",
                1,
            ).otherwise(0)
        ).alias("passed_columns"),
        F.sum(
            F.when(
                F.col("validation_status") != "PASSED",
                1,
            ).otherwise(0)
        ).alias("failed_columns"),
    )
    .first()
)

total_columns = validation_summary["total_columns"]
passed_columns = validation_summary["passed_columns"]
failed_columns = validation_summary["failed_columns"]

display(
    schema_validation_results.orderBy(
        "ordinal_position"
    )
)

if total_columns != len(expected_schema):
    raise RuntimeError(
        "Schema validation produced an unexpected "
        "number of result rows. "
        f"Expected: {len(expected_schema)}. "
        f"Actual: {total_columns}."
    )

if failed_columns != 0:
    failed_results = (
        schema_validation_results
        .filter(
            F.col("validation_status") != "PASSED"
        )
        .orderBy("ordinal_position")
    )

    display(failed_results)

    raise RuntimeError(
        "Schema validation failed. "
        f"Mismatches detected: {failed_columns}."
    )

(
    schema_validation_results
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(validation_results_table)
)

persisted_results = spark.table(
    validation_results_table
)

persisted_summary = (
    persisted_results
    .agg(
        F.count("*").alias("total_columns"),
        F.sum(
            F.when(
                F.col("validation_status") == "PASSED",
                1,
            ).otherwise(0)
        ).alias("passed_columns"),
        F.sum(
            F.when(
                F.col("validation_status") != "PASSED",
                1,
            ).otherwise(0)
        ).alias("failed_columns"),
        F.countDistinct(
            "ordinal_position"
        ).alias("distinct_positions"),
    )
    .first()
)

if (
    persisted_summary["total_columns"]
    != len(expected_schema)
):
    raise RuntimeError(
        "Persisted schema validation results do not "
        "contain exactly 23 rows."
    )

if (
    persisted_summary["distinct_positions"]
    != len(expected_schema)
):
    raise RuntimeError(
        "Persisted schema validation results contain "
        "duplicate or missing ordinal positions."
    )

if persisted_summary["failed_columns"] != 0:
    raise RuntimeError(
        "Persisted schema validation results contain "
        "failed columns."
    )

validation_completed_at = datetime.now(
    timezone.utc
).replace(tzinfo=None)

result_payload = {
    "status": "PASSED",
    "source_table": source_table_display_name,
    "source_period": source_period,
    "total_columns": total_columns,
    "passed_columns": passed_columns,
    "failed_columns": failed_columns,
    "validation_started_at": (
        validation_started_at.isoformat()
    ),
    "validation_completed_at": (
        validation_completed_at.isoformat()
    ),
}

print(
    f"Schema validation PASSED: "
    f"{passed_columns} of {total_columns} columns."
)
print(json.dumps(result_payload, indent=2))

notebookutils.notebook.exit(
    json.dumps(result_payload)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
