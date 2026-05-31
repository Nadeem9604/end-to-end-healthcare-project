# Databricks notebook source
from pyspark.sql.functions import sha2, col, current_timestamp, monotonically_increasing_id

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC create schema if not exists healthcareextcatlog.silver;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from healthcareextcatlog.bronze.diagnosis_raw;

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
from delta.tables import DeltaTable

bronze_table = "healthcareextcatlog.bronze.diagnosis_raw"
silver_table = "healthcareextcatlog.silver.dim_diagnosis"
checkpoint_path = "abfss://data@healthcareproject88.dfs.core.windows.net/silver/dim_diagnosis/checkpoint/"

# Read stream
df_diagnosis_bronze = spark.readStream.table(bronze_table)

# Transform
df_patient_clean = (
    df_diagnosis_bronze
        .dropDuplicates(["diagnosis_code"])
        .withColumn("load_timestamp", current_timestamp())
)

# Merge function
def merge_dim_diagnosis(batch_df, batch_id):
    if not spark.catalog.tableExists(silver_table):
        batch_df.write.format("delta").mode("overwrite").saveAsTable(silver_table)
        return

    dim_diagnosis = DeltaTable.forName(spark, silver_table)

    (dim_diagnosis.alias("t")
        .merge(
            batch_df.alias("s"),
            "t.diagnosis_code = s.diagnosis_code"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

# Write stream
(
    df_patient_clean.writeStream
        .foreachBatch(merge_dim_diagnosis)
        .outputMode("update")   # important fix
        .trigger(availableNow=True)
        .option("checkpointLocation", checkpoint_path)
        .start()
)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from healthcareextcatlog.silver.dim_diagnosis;