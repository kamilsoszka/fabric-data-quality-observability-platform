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

import os
import requests

source_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2026-01.parquet"
target_dir = "/lakehouse/default/Files/01_raw"
target_file = f"{target_dir}/yellow_tripdata_2026-01.parquet"

os.makedirs(target_dir, exist_ok=True)

with requests.get(source_url, stream=True, timeout=300) as response:
    response.raise_for_status()

    with open(target_file, "wb") as file:
        for chunk in response.iter_content(chunk_size=8 * 1024 * 1024):
            if chunk:
                file.write(chunk)

file_size_mb = os.path.getsize(target_file) / (1024 * 1024)

print(f"Downloaded file: {target_file}")
print(f"File size: {file_size_mb:.2f} MB")

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
