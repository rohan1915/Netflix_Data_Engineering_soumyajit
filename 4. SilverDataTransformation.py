# Databricks notebook source
# MAGIC %md
# MAGIC ## Silver Data Transformation

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

# COMMAND ----------

df = spark.read.format("delta")\
    .option("header", "true")\
    .option("inferSchema", "true")\
    .load("abfss://bronze@ssnetflixdl.dfs.core.windows.net/netflix_titles")
display(df)

# COMMAND ----------

df = df.fillna({'duration_minutes': 0, 'duration_seasons': 1})

# COMMAND ----------

display(df)

# COMMAND ----------

df =df.withColumn('shorttitle',split(col('title'), ':')[0])
display(df)

# COMMAND ----------

df =df.withColumn('rating',split(col('rating'), '-')[0])
display(df)

# COMMAND ----------

df =df.withColumn('type_flag',when(col('type')=='TV Show',2)\
    .when(col('type')=='Movie',1)\
    .otherwise(0))
display(df)

# COMMAND ----------

df = df.withColumn('duration_ranking',dense_rank().over(Window.orderBy(col('duration_minutes').desc()))) #using dense rank function
display(df)

# COMMAND ----------

df.createOrReplaceTempView("temp")  # create a temp view to convert the dataframe to a temporary table to run sql statement. It's scope is limited to this notebook only. For Global temo view use the GlobalTempView option. It can be accessed from other notebooks but it will be terminated on the session close.

# df.createOrReplaceGlobalTempView("global_view"). select * from global_temp.global_view where type = Movie;

df = spark.sql("""
SELECT * FROM temp where duration_minutes like ('%ing%');
""")
display(df)

# COMMAND ----------

df_vis = df.groupBy('type').agg(count('type').alias('count'))
display(df_vis)

# COMMAND ----------

df.write.format("delta").mode("overwrite").save("abfss://silver@ssnetflixdl.dfs.core.windows.net/netflix_titles")

# COMMAND ----------

