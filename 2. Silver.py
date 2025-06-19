# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Silver Notebook

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

dbutils.widgets.text("sourcefolder",'netflix_directors') #parameters, default value
dbutils.widgets.text("targetfolder",'netfilx_directors')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Variables

# COMMAND ----------

var_src_folder = dbutils.widgets.get("sourcefolder") #storing the parameters in variable
var_tgt_folder = dbutils.widgets.get("targetfolder")

# COMMAND ----------

df = spark.read.format("csv")\
  .option("header", "true")\
  .option("inferSchema", "true")\
  .load(f"abfss://bronze@ssnetflixdl.dfs.core.windows.net/{var_src_folder}") #using the variable to pass the foldername

# COMMAND ----------

# display(df)

# COMMAND ----------

df.write.format("delta")\
    .mode("overwrite")\
    .save(f"abfss://silver@ssnetflixdl.dfs.core.windows.net/{var_tgt_folder}")

# COMMAND ----------

