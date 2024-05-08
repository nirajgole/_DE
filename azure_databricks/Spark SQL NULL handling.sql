-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### NULL Handling
-- MAGIC By default if we try to concat or add to another column or literal it will result in null.\
-- MAGIC nvl, nvl2\
-- MAGIC Coalesce returns first not null values if we pass multiple arguments.\
-- MAGIC CASE-WHEN-ELSE-END can be use for any conditional logic

-- COMMAND ----------

select null = null as result

-- COMMAND ----------

select null is null

-- COMMAND ----------

select 1+null as result

-- COMMAND ----------

desc function nvl

-- COMMAND ----------

select coalesce(null,0,'James' ) as result

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### CASE-WHEN-ELSE-END

-- COMMAND ----------

select nvl2(null,'TRUE' ,'FALSE' ) as result

-- COMMAND ----------

select
  case when null
    then 'TRUE'
    else 'FALSE'
  end as result
from ''

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/departuredelays.csv

-- COMMAND ----------

-- mode "FAILFAST" aborts file parsing with a RuntimeException if malformed lines are encountered
SELECT *,
  CASE WHEN delay > 0 AND _rescued_data is null THEN 'not delay'
  ELSE 'delay'
  END AS is_delayed
  FROM read_files(
  'dbfs:/departuredelays.csv',
  format => 'csv',
  header => true,
  mode => 'FAILFAST')
