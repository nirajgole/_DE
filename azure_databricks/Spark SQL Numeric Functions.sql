-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### Numeric Functions
-- MAGIC abs\
-- MAGIC sum, avg, count, count_if\
-- MAGIC round, ceil, floor\
-- MAGIC greatest\
-- MAGIC min, max\
-- MAGIC rand\
-- MAGIC pow, sqrt\
-- MAGIC cumedist, stddev, variance

-- COMMAND ----------

select abs(-10.10)

-- COMMAND ----------

select '09'

-- COMMAND ----------

select 09

-- COMMAND ----------

desc FUNCTION cast

-- COMMAND ----------

select cast('0.04' as float) as result

-- COMMAND ----------

select cast('0.04' as int) as zero