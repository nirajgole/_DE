-- Databricks notebook source
DROP DATABASE IF EXISTS ng_db_bronze CASCADE

-- COMMAND ----------

-- %python
-- dbutils.fs.rm(dbutils.widgets.get('bronze_base'), recurse=True)

-- COMMAND ----------

-- %python
-- dbutils.fs.rm(dbutils.widgets.get('gold_base'), recurse=True)

-- COMMAND ----------

