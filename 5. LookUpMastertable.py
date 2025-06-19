# Databricks notebook source
dbutils.widgets.text("weekdays", "7")

# COMMAND ----------

day = int(dbutils.widgets.get("weekdays"))

# COMMAND ----------

dbutils.jobs.taskValues.set("dayoutput", day)