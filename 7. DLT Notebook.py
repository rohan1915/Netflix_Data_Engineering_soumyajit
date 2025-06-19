# Databricks notebook source
# MAGIC %md
# MAGIC # DLT Notebook Gold Layer

# COMMAND ----------

import dlt
#defining the expect data quality criteria to filter the data
criteria = {

    "rule1" : "show_id is NOT NULL"
}

# COMMAND ----------

@dlt.table(

    name = "gold_netflixdirectors"
) # defining the table name in decorator otherwise the function name will be considered as the table name

@dlt.expect_all_or_drop(criteria)
def func1():
    df = spark.readStream.format("delta").load("abfss://silver@ssnetflixdl.dfs.core.windows.net/netflix_directors")
    return df

# COMMAND ----------

@dlt.table(

    name = "gold_netflixcast"
)

@dlt.expect_all_or_drop(criteria)
def func1():
    df = spark.readStream.format("delta").load("abfss://silver@ssnetflixdl.dfs.core.windows.net/netflix_cast")
    return df

# COMMAND ----------

@dlt.table(

    name = "gold_netflixcountries"
)

@dlt.expect_all_or_drop(criteria)
def func1():
    df = spark.readStream.format("delta").load("abfss://silver@ssnetflixdl.dfs.core.windows.net/netflix_countries")
    return df

# COMMAND ----------

@dlt.table(

    name = "gold_netflixcategory"
)

@dlt.expect_all_or_drop(criteria)
def func1():
    df = spark.readStream.format("delta").load\
    ("abfss://silver@ssnetflixdl.dfs.core.windows.net/netflix_category")
    return df

# COMMAND ----------

@dlt.table()

def gold_stg_netflixtitles(): # creating a streaming staging table to read the data from 
    df = spark.readStream.format("delta").load \
    ("abfss://silver@ssnetflixdl.dfs.core.windows.net/netflix_titles")
    return df

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

@dlt.view()

def gold_trns_netflixtitles(): # creating a streaming transformed view to read the data from
    df = spark.readStream.table("LIVE.gold_stg_netflixtitles")
    df = df.withColumn('newflag',lit(1))    
    return df

# COMMAND ----------

masterdata_rules = {

    "rule1" : "newflag is NOT NULL",
    "rule2" : "show_id is NOT NULL"
}

# COMMAND ----------

@dlt.table()

@dlt.expect_all_or_drop(masterdata_rules)
def gold_netflixtitles(): # creating a streaming gold table to read the data from
    df = spark.readStream.table("LIVE.gold_trns_netflixtitles")
    return df