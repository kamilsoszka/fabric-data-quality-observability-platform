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
import os
import shutil
from datetime import datetime, timezone

import pyarrow.parquet as pq
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from pyspark.sql import functions as F
from pyspark.sql import types as T


# Define ingestion configuration
source_url = (
    "https://d37ci6vzurychx.cloudfront.net/trip-data/"
    "yellow_tripdata_2026-01.parquet"
)
source_file_name = "yellow_tripdata_2026-01.parquet"
source_period = "2026-01"

local_temporary_file = "/tmp/yellow_tripdata_2026-01.download.parquet"
onelake_directory = "/lakehouse/default/Files/01_raw"
onelake_target_file = f"{onelake_directory}/{source_file_name}"
onelake_staging_file = (
    f"{onelake_directory}/yellow_tripdata_2026-01.staging.parquet"
)
spark_source_path = f"Files/01_raw/{source_file_name}"

bronze_table = "`01_bronze`.yellow_taxi_trips"
bronze_table_display_name = "01_bronze.yellow_taxi_trips"

expected_source_record_count = 3_724_889
expected_source_column_count = 20
expected_bronze_column_count = 23

download_timeout_seconds = 300
download_chunk_size_bytes = 8 * 1024 * 1024
minimum_file_size_bytes = 1 * 1024 * 1024

required_source_columns = [
    "VendorID",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "RatecodeID",
    "store_and_fwd_flag",
    "PULocationID",
    "DOLocationID",
    "payment_type",
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "total_amount",
    "congestion_surcharge",
    "Airport_fee",
]

technical_columns = [
    "_ingested_at",
    "_source_file",
    "_source_period",
]

datetime_columns = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
]


def remove_file_if_present(file_path):
    if os.path.exists(file_path):
        os.remove(file_path)


def create_http_session():
    retry_strategy = Retry(
        total=4,
        connect=4,
        read=4,
        status=4,
        backoff_factor=2,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=frozenset(["GET"]),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session = requests.Session()
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def validate_required_columns(actual_columns, required_columns, dataset_name):
    missing_columns = sorted(set(required_columns) - set(actual_columns))
    if missing_columns:
        raise RuntimeError(
            f"{dataset_name} is missing required columns: {missing_columns}"
        )


def download_to_local_file():
    remove_file_if_present(local_temporary_file)
    downloaded_bytes = 0
    expected_content_length = None
    session = create_http_session()

    try:
        with session.get(
            source_url,
            stream=True,
            timeout=download_timeout_seconds,
        ) as response:
            response.raise_for_status()
            content_length_header = response.headers.get("Content-Length")
            if content_length_header is not None:
                expected_content_length = int(content_length_header)

            with open(local_temporary_file, "wb") as output_file:
                for chunk in response.iter_content(
                    chunk_size=download_chunk_size_bytes
                ):
                    if not chunk:
                        continue
                    output_file.write(chunk)
                    downloaded_bytes += len(chunk)
                output_file.flush()
                os.fsync(output_file.fileno())
    finally:
        session.close()

    if not os.path.exists(local_temporary_file):
        raise RuntimeError("The local temporary file was not created.")

    actual_file_size = os.path.getsize(local_temporary_file)

    if actual_file_size != downloaded_bytes:
        raise RuntimeError(
            "Downloaded byte count does not match the local file size. "
            f"Downloaded: {downloaded_bytes}. File size: {actual_file_size}."
        )

    if actual_file_size < minimum_file_size_bytes:
        raise RuntimeError(
            "Downloaded file is unexpectedly small. "
            f"Actual: {actual_file_size} bytes. "
            f"Minimum: {minimum_file_size_bytes} bytes."
        )

    if (
        expected_content_length is not None
        and actual_file_size != expected_content_length
    ):
        raise RuntimeError(
            "Downloaded file size does not match Content-Length. "
            f"Expected: {expected_content_length}. "
            f"Actual: {actual_file_size}."
        )

    return {
        "downloaded_bytes": downloaded_bytes,
        "content_length": expected_content_length,
        "file_size": actual_file_size,
    }


def validate_local_parquet():
    try:
        parquet_file = pq.ParquetFile(local_temporary_file)
    except Exception as error:
        raise RuntimeError(
            "The downloaded local file is not a valid Parquet file."
        ) from error

    source_columns = parquet_file.schema_arrow.names
    source_column_count = len(source_columns)
    source_record_count = parquet_file.metadata.num_rows

    if source_column_count != expected_source_column_count:
        raise RuntimeError(
            "Source column count does not match the expected contract. "
            f"Expected: {expected_source_column_count}. "
            f"Actual: {source_column_count}. Columns: {source_columns}."
        )

    if len(set(source_columns)) != source_column_count:
        raise RuntimeError("Source Parquet file contains duplicate column names.")

    validate_required_columns(
        source_columns,
        required_source_columns,
        "Source Parquet file",
    )

    reserved_columns = sorted(set(source_columns) & set(technical_columns))
    if reserved_columns:
        raise RuntimeError(
            "Source Parquet file contains reserved technical columns: "
            f"{reserved_columns}"
        )

    if source_record_count != expected_source_record_count:
        raise RuntimeError(
            "Source record count does not match the expected contract. "
            f"Expected: {expected_source_record_count}. "
            f"Actual: {source_record_count}."
        )

    return {
        "source_columns": source_columns,
        "source_column_count": source_column_count,
        "source_record_count": source_record_count,
        "row_group_count": parquet_file.metadata.num_row_groups,
    }


def publish_file_to_onelake():
    os.makedirs(onelake_directory, exist_ok=True)
    remove_file_if_present(onelake_staging_file)
    shutil.copyfile(local_temporary_file, onelake_staging_file)

    local_size = os.path.getsize(local_temporary_file)
    staging_size = os.path.getsize(onelake_staging_file)

    if staging_size != local_size:
        raise RuntimeError(
            "OneLake staging file size differs from the validated local file. "
            f"Local: {local_size}. Staging: {staging_size}."
        )

    os.replace(onelake_staging_file, onelake_target_file)

    if not os.path.exists(onelake_target_file):
        raise RuntimeError(
            "The OneLake target file does not exist after replacement."
        )

    target_size = os.path.getsize(onelake_target_file)
    if target_size != local_size:
        raise RuntimeError(
            "OneLake target file size differs from the validated local file. "
            f"Local: {local_size}. Target: {target_size}."
        )

    return target_size


def read_source_with_spark(expected_columns, expected_record_count):
    try:
        source_dataframe = spark.read.format("parquet").load(spark_source_path)
    except Exception as error:
        raise RuntimeError(
            "Spark cannot read the published Parquet file through the "
            f"default Lakehouse relative path: {spark_source_path}"
        ) from error

    if source_dataframe.columns != expected_columns:
        raise RuntimeError(
            "Spark source column order differs from the locally validated "
            f"Parquet contract. Expected: {expected_columns}. "
            f"Actual: {source_dataframe.columns}."
        )

    actual_record_count = source_dataframe.count()
    if actual_record_count != expected_record_count:
        raise RuntimeError(
            "Spark source record count differs from the locally validated "
            f"Parquet contract. Expected: {expected_record_count}. "
            f"Actual: {actual_record_count}."
        )

    return source_dataframe


def create_bronze_dataframe(source_dataframe, ingestion_timestamp):
    # Cast TIMESTAMP_NTZ source fields to Spark TimestampType so that the
    # Lakehouse SQL analytics endpoint exposes both datetime columns.
    return (
        source_dataframe
        .withColumn(
            "tpep_pickup_datetime",
            F.col("tpep_pickup_datetime").cast(T.TimestampType()),
        )
        .withColumn(
            "tpep_dropoff_datetime",
            F.col("tpep_dropoff_datetime").cast(T.TimestampType()),
        )
        .withColumn(
            "_ingested_at",
            F.lit(ingestion_timestamp).cast(T.TimestampType()),
        )
        .withColumn("_source_file", F.lit(source_file_name))
        .withColumn("_source_period", F.lit(source_period))
    )


def validate_bronze_before_write(
    bronze_dataframe,
    source_columns,
    expected_record_count,
):
    expected_columns = source_columns + technical_columns

    if bronze_dataframe.columns != expected_columns:
        raise RuntimeError(
            "Bronze column order does not match the expected contract. "
            f"Expected: {expected_columns}. "
            f"Actual: {bronze_dataframe.columns}."
        )

    if len(bronze_dataframe.columns) != expected_bronze_column_count:
        raise RuntimeError(
            "Bronze column count does not match the expected contract. "
            f"Expected: {expected_bronze_column_count}. "
            f"Actual: {len(bronze_dataframe.columns)}."
        )

    for column_name in datetime_columns + ["_ingested_at"]:
        actual_type = bronze_dataframe.schema[column_name].dataType
        if not isinstance(actual_type, T.TimestampType):
            raise RuntimeError(
                f"{column_name} must be TimestampType before Bronze write. "
                f"Actual type: {actual_type.simpleString()}."
            )

    actual_record_count = bronze_dataframe.count()
    if actual_record_count != expected_record_count:
        raise RuntimeError(
            "Bronze pre-write record count differs from the source count. "
            f"Expected: {expected_record_count}. Actual: {actual_record_count}."
        )

    validation_row = (
        bronze_dataframe
        .agg(
            *[
                F.sum(
                    F.when(F.col(column_name).isNull(), 1).otherwise(0)
                ).alias(f"null_{column_name}")
                for column_name in technical_columns
            ]
        )
        .first()
    )

    if any(validation_row[f"null_{name}"] != 0 for name in technical_columns):
        raise RuntimeError("Bronze technical metadata contains NULL values.")

    return expected_columns


def write_and_validate_bronze(
    bronze_dataframe,
    expected_columns,
    expected_record_count,
    ingestion_timestamp,
):
    (
        bronze_dataframe.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(bronze_table)
    )

    persisted_bronze = spark.table(bronze_table)

    if persisted_bronze.columns != expected_columns:
        raise RuntimeError(
            "Persisted Bronze columns differ from the validated pre-write "
            f"contract. Expected: {expected_columns}. "
            f"Actual: {persisted_bronze.columns}."
        )

    for column_name in datetime_columns + ["_ingested_at"]:
        actual_type = persisted_bronze.schema[column_name].dataType
        if not isinstance(actual_type, T.TimestampType):
            raise RuntimeError(
                f"Persisted {column_name} must be TimestampType. "
                f"Actual type: {actual_type.simpleString()}."
            )

    persisted_record_count = persisted_bronze.count()
    if persisted_record_count != expected_record_count:
        raise RuntimeError(
            "Persisted Bronze count differs from the validated source count. "
            f"Expected: {expected_record_count}. Actual: {persisted_record_count}."
        )

    persisted_metadata = (
        persisted_bronze
        .agg(
            F.countDistinct("_ingested_at").alias("ingested_at_count"),
            F.min("_ingested_at").alias("minimum_ingested_at"),
            F.max("_ingested_at").alias("maximum_ingested_at"),
            F.countDistinct("_source_file").alias("source_file_count"),
            F.first("_source_file").alias("source_file"),
            F.countDistinct("_source_period").alias("source_period_count"),
            F.first("_source_period").alias("source_period"),
        )
        .first()
    )

    if persisted_metadata["ingested_at_count"] != 1:
        raise RuntimeError("Persisted Bronze must contain one _ingested_at value.")

    if (
        persisted_metadata["minimum_ingested_at"] != ingestion_timestamp
        or persisted_metadata["maximum_ingested_at"] != ingestion_timestamp
    ):
        raise RuntimeError(
            "Persisted _ingested_at does not match the current ingestion run."
        )

    if (
        persisted_metadata["source_file_count"] != 1
        or persisted_metadata["source_file"] != source_file_name
    ):
        raise RuntimeError("Persisted _source_file is invalid.")

    if (
        persisted_metadata["source_period_count"] != 1
        or persisted_metadata["source_period"] != source_period
    ):
        raise RuntimeError("Persisted _source_period is invalid.")

    return persisted_record_count, persisted_bronze.schema.jsonValue()


ingestion_started_at = datetime.now(timezone.utc).replace(tzinfo=None)

print(f"Ingestion started at: {ingestion_started_at}")
print(f"Source URL: {source_url}")
print(f"Local temporary file: {local_temporary_file}")
print(f"OneLake target file: {onelake_target_file}")
print(f"Spark source path: {spark_source_path}")
print(f"Bronze table: {bronze_table_display_name}")

try:
    download_result = download_to_local_file()
    parquet_validation = validate_local_parquet()
    target_file_size = publish_file_to_onelake()

    source_dataframe = read_source_with_spark(
        expected_columns=parquet_validation["source_columns"],
        expected_record_count=parquet_validation["source_record_count"],
    )

    bronze_dataframe = create_bronze_dataframe(
        source_dataframe=source_dataframe,
        ingestion_timestamp=ingestion_started_at,
    )

    expected_bronze_columns = validate_bronze_before_write(
        bronze_dataframe=bronze_dataframe,
        source_columns=parquet_validation["source_columns"],
        expected_record_count=parquet_validation["source_record_count"],
    )

    persisted_record_count, persisted_schema = write_and_validate_bronze(
        bronze_dataframe=bronze_dataframe,
        expected_columns=expected_bronze_columns,
        expected_record_count=parquet_validation["source_record_count"],
        ingestion_timestamp=ingestion_started_at,
    )

    ingestion_completed_at = datetime.now(timezone.utc).replace(tzinfo=None)
    execution_duration_seconds = (
        ingestion_completed_at - ingestion_started_at
    ).total_seconds()

    result_payload = {
        "status": "PASSED",
        "source_url": source_url,
        "source_file": source_file_name,
        "source_period": source_period,
        "spark_source_path": spark_source_path,
        "bronze_table": bronze_table_display_name,
        "source_record_count": parquet_validation["source_record_count"],
        "bronze_record_count": persisted_record_count,
        "source_column_count": parquet_validation["source_column_count"],
        "bronze_column_count": len(expected_bronze_columns),
        "bronze_datetime_types": {
            column_name: spark.table(bronze_table)
            .schema[column_name]
            .dataType.simpleString()
            for column_name in datetime_columns
        },
        "row_group_count": parquet_validation["row_group_count"],
        "downloaded_bytes": download_result["downloaded_bytes"],
        "content_length": download_result["content_length"],
        "target_file_size": target_file_size,
        "ingested_at": ingestion_started_at.isoformat(),
        "completed_at": ingestion_completed_at.isoformat(),
        "execution_duration_seconds": execution_duration_seconds,
    }

except Exception as error:
    remove_file_if_present(local_temporary_file)
    remove_file_if_present(onelake_staging_file)

    failure_payload = {
        "status": "FAILED",
        "source_url": source_url,
        "source_file": source_file_name,
        "source_period": source_period,
        "spark_source_path": spark_source_path,
        "bronze_table": bronze_table_display_name,
        "error_type": type(error).__name__,
        "error_message": str(error),
        "failed_at": datetime.now(timezone.utc).isoformat(),
    }

    print(json.dumps(failure_payload, indent=2))
    raise

remove_file_if_present(local_temporary_file)
remove_file_if_present(onelake_staging_file)

print("Bronze ingestion validation PASSED.")
print(json.dumps(result_payload, indent=2))

notebookutils.notebook.exit(json.dumps(result_payload))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

source_file = "Files/01_raw/yellow_tripdata_2026-01.parquet"

df_source = spark.read.parquet(source_file)

print(f"Number of records: {df_source.count():,}")
print(f"Number of columns: {len(df_source.columns)}")

display(df_source.limit(10))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

bronze_table = "`01_bronze`.yellow_taxi_trips"

df_bronze = (
    df_source
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn(
        "_source_file",
        F.lit("yellow_tripdata_2026-01.parquet")
    )
    .withColumn(
        "_source_period",
        F.lit("2026-01")
    )
)

(
    df_bronze.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(bronze_table)
)

print(f"Bronze table created: {bronze_table}")
print(f"Number of records written: {spark.table(bronze_table).count():,}")
print(f"Number of columns written: {len(spark.table(bronze_table).columns)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
