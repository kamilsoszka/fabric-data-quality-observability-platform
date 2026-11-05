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

bronze_table = "`01_bronze`.yellow_taxi_trips"

df_bronze = spark.table(bronze_table)

df_bronze.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

null_profile = df_bronze.select([
    F.sum(F.col(column).isNull().cast("long")).alias(column)
    for column in df_bronze.columns
])

display(null_profile)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

total_rows = df_bronze.count()

null_profile_vertical = (
    df_bronze
    .select([
        F.sum(F.col(column).isNull().cast("long")).alias(column)
        for column in df_bronze.columns
    ])
    .select(
        F.explode(
            F.array(*[
                F.struct(
                    F.lit(column).alias("column_name"),
                    F.col(column).alias("null_count")
                )
                for column in df_bronze.columns
            ])
        ).alias("profile")
    )
    .select(
        F.col("profile.column_name"),
        F.col("profile.null_count"),
        F.round(
            F.col("profile.null_count") / F.lit(total_rows) * 100,
            2
        ).alias("null_percentage")
    )
    .orderBy(
        F.desc("null_count"),
        F.asc("column_name")
    )
)

display(null_profile_vertical)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

columns_with_nulls = [
    "passenger_count",
    "RatecodeID",
    "store_and_fwd_flag",
    "congestion_surcharge",
    "Airport_fee"
]

null_pattern = (
    df_bronze
    .groupBy(*[
        F.col(column).isNull().alias(f"{column}_is_null")
        for column in columns_with_nulls
    ])
    .count()
    .orderBy(F.desc("count"))
)

display(null_pattern)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

vendor_null_profile = (
    df_bronze
    .withColumn(
        "five_fields_missing",
        F.col("passenger_count").isNull()
        & F.col("RatecodeID").isNull()
        & F.col("store_and_fwd_flag").isNull()
        & F.col("congestion_surcharge").isNull()
        & F.col("Airport_fee").isNull()
    )
    .groupBy("VendorID", "five_fields_missing")
    .count()
    .orderBy("VendorID", "five_fields_missing")
)

display(vendor_null_profile)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

daily_null_profile = (
    df_bronze
    .withColumn(
        "pickup_date",
        F.to_date("tpep_pickup_datetime")
    )
    .withColumn(
        "five_fields_missing",
        F.col("passenger_count").isNull()
        & F.col("RatecodeID").isNull()
        & F.col("store_and_fwd_flag").isNull()
        & F.col("congestion_surcharge").isNull()
        & F.col("Airport_fee").isNull()
    )
    .groupBy("pickup_date")
    .agg(
        F.count("*").alias("total_records"),
        F.sum(
            F.col("five_fields_missing").cast("long")
        ).alias("missing_records")
    )
    .withColumn(
        "missing_percentage",
        F.round(
            F.col("missing_records") / F.col("total_records") * 100,
            2
        )
    )
    .orderBy("pickup_date")
)

display(daily_null_profile)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

quality_rules = F.array(
    F.when(F.col("VendorID").isNull(), "VENDOR_ID_NULL"),
    F.when(F.col("tpep_pickup_datetime").isNull(), "PICKUP_DATETIME_NULL"),
    F.when(F.col("tpep_dropoff_datetime").isNull(), "DROPOFF_DATETIME_NULL"),
    F.when(
        F.col("tpep_dropoff_datetime") < F.col("tpep_pickup_datetime"),
        "DROPOFF_BEFORE_PICKUP"
    ),
    F.when(F.col("trip_distance") < 0, "NEGATIVE_TRIP_DISTANCE"),
    F.when(F.col("PULocationID").isNull(), "PICKUP_LOCATION_NULL"),
    F.when(F.col("DOLocationID").isNull(), "DROPOFF_LOCATION_NULL"),
    F.when(F.col("fare_amount").isNull(), "FARE_AMOUNT_NULL"),
    F.when(F.col("total_amount").isNull(), "TOTAL_AMOUNT_NULL")
)

df_validated = (
    df_bronze
    .withColumn(
        "_failed_rules",
        F.filter(quality_rules, lambda rule: rule.isNotNull())
    )
    .withColumn("_quality_checked_at", F.current_timestamp())
    .withColumn(
        "_quality_status",
        F.when(F.size("_failed_rules") == 0, "PASSED")
         .otherwise("QUARANTINED")
    )
)

df_silver = (
    df_validated
    .filter(F.col("_quality_status") == "PASSED")
)

df_quarantine = (
    df_validated
    .filter(F.col("_quality_status") == "QUARANTINED")
)

df_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("`02_silver`.yellow_taxi_trips")

df_quarantine.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("`03_quarantine`.yellow_taxi_trips")

monitoring_result = (
    df_validated
    .groupBy("_quality_status")
    .count()
    .withColumn("source_table", F.lit("01_bronze.yellow_taxi_trips"))
    .withColumn("quality_run_at", F.current_timestamp())
    .withColumn("source_period", F.lit("2026-01"))
)

monitoring_result.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("`04_monitoring`.quality_run_summary")

display(monitoring_result)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

rule_results = (
    df_validated
    .select(F.explode_outer("_failed_rules").alias("rule_name"))
    .filter(F.col("rule_name").isNotNull())
    .groupBy("rule_name")
    .count()
    .withColumnRenamed("count", "failed_records")
    .withColumn("total_records", F.lit(df_validated.count()))
    .withColumn(
        "failure_percentage",
        F.round(F.col("failed_records") / F.col("total_records") * 100, 4)
    )
    .withColumn("source_period", F.lit("2026-01"))
    .withColumn("quality_run_at", F.current_timestamp())
)

rule_results.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("`04_monitoring`.quality_rule_results")

display(rule_results)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

freshness_sla = (
    df_bronze
    .agg(
        F.max("_ingested_at").alias("last_ingested_at"),
        F.count("*").alias("record_count")
    )
    .withColumn("checked_at", F.current_timestamp())
    .withColumn(
        "age_hours",
        F.round(
            (F.unix_timestamp("checked_at")
             - F.unix_timestamp("last_ingested_at")) / 3600,
            2
        )
    )
    .withColumn("sla_threshold_hours", F.lit(24))
    .withColumn(
        "sla_status",
        F.when(F.col("age_hours") <= 24, "PASSED")
         .otherwise("FAILED")
    )
    .withColumn("source_period", F.lit("2026-01"))
)

freshness_sla.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("`04_monitoring`.freshness_sla_results")

display(freshness_sla)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

negative_test = (
    spark.createDataFrame(
        [("2026-01-10 12:00:00", "2026-01-10 11:00:00")],
        ["pickup", "dropoff"]
    )
    .select(
        F.to_timestamp("pickup").alias("tpep_pickup_datetime"),
        F.to_timestamp("dropoff").alias("tpep_dropoff_datetime")
    )
    .withColumn(
        "detected_rule",
        F.when(
            F.col("tpep_dropoff_datetime") < F.col("tpep_pickup_datetime"),
            "DROPOFF_BEFORE_PICKUP"
        )
    )
)

display(negative_test)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
