# Databricks notebook source

source_path = "abfss://data@healthcareproject88.dfs.core.windows.net/staging/patients/"
checkpoint_path = "abfss://data@healthcareproject88.dfs.core.windows.net/bronze/patients_raw/checkpoint/"
schema_location = "abfss://data@healthcareproject88.dfs.core.windows.net/bronze/patients_raw/schema/"

# Autoloader read
df = (spark.readStream
          .format("cloudFiles")
          .option("cloudFiles.format", "csv")
          .option("header", "true")
          .option("inferSchema", "true")
          .option("cloudFiles.maxFilesPerTrigger", 1) # READ ONE FILE AT A TIME
          .option("cloudFiles.schemaLocation", schema_location)  
          .load(source_path)
     )

# Write Bronze table (append)
(
    df.drop("_rescued_data")
      .writeStream
      .format("delta")
      .option("checkpointLocation", checkpoint_path)
      .outputMode("append")
      .trigger(availableNow=True)   # AVAILABLE NOW → ingests all files & stops
      .toTable("healthcareextcatlog.bronze.patients_raw")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from healthcareextcatlog.bronze.patients_raw