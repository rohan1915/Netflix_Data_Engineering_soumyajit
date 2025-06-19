# Databricks notebook source
var = dbutils.jobs.taskValues.get(taskKey = 'WeekdayLookup',key = 'dayoutput',debugValue='test')

# COMMAND ----------

print(var)

# COMMAND ----------

