# Databricks notebook source
# MAGIC %md
# MAGIC # Incremental Data Loading using Auto Loader

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema netflix_catalog.net_schema;

# COMMAND ----------

checkpoint_location = "abfss://silver@ssnetflixdl.dfs.core.windows.net/checkpoint"

# COMMAND ----------

df = spark.readStream\
  .format("cloudFiles")\
  .option("cloudFiles.format", "csv")\
  .option("cloudFiles.schemaLocation", checkpoint_location)\
  .load("abfss://raw@ssnetflixdl.dfs.core.windows.net")

# COMMAND ----------

display(df)

# COMMAND ----------

df.writeStream\
  .option("checkpointLocation", checkpoint_location)\
  .trigger(availableNow=True)\
  .start("abfss://bronze@ssnetflixdl.dfs.core.windows.net/netflix_titles")

# COMMAND ----------

