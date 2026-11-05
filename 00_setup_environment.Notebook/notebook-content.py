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

# MAGIC %%sql
# MAGIC 
# MAGIC DROP SCHEMA IF EXISTS bronze;
# MAGIC DROP SCHEMA IF EXISTS silver;
# MAGIC DROP SCHEMA IF EXISTS quarantine;
# MAGIC DROP SCHEMA IF EXISTS monitoring;
# MAGIC 
# MAGIC CREATE SCHEMA IF NOT EXISTS `01_bronze`;
# MAGIC CREATE SCHEMA IF NOT EXISTS `02_silver`;
# MAGIC CREATE SCHEMA IF NOT EXISTS `03_quarantine`;
# MAGIC CREATE SCHEMA IF NOT EXISTS `04_monitoring`;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SHOW SCHEMAS;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
