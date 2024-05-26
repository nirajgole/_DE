-- Databricks notebook source
DESCRIBE FUNCTION current_date

-- COMMAND ----------

select current_date as current_date

-- COMMAND ----------

describe function substring

-- COMMAND ----------

describe function substr

-- COMMAND ----------

describe function current_database

-- COMMAND ----------

-- MAGIC %md
-- MAGIC String Manipulation Function
-- MAGIC
-- MAGIC Case Conversion - lower, upper, initicap\
-- MAGIC Getting Size of the column value - length\
-- MAGIC Extracting Data - substr, split\
-- MAGIC Trimming and Padding - trim, ltrim, rtrim, rpad and lpad\
-- MAGIC Reversing Strings - reverse\
-- MAGIC Concatenating multiple strings - concat and concat_ws\
-- MAGIC Array into multiple rows - explode

-- COMMAND ----------

select trim('ab' from 'aaaaaaHello Worldbbbbbb') as result

-- COMMAND ----------

select concat_ws('-','Hello','World') as result

-- COMMAND ----------

select concat_ws('-',array('Hello','World'))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Date Manipulation Functions
-- MAGIC - Date Arithmetic date_add - date_add
-- MAGIC - Getting beginning date or time - trunc or date_trunc
-- MAGIC - Extracting information using date_format as well as calendar functions
-- MAGIC - Dealing with unix timestamp - from_unixtime, to_unix_timestamp
-- MAGIC

-- COMMAND ----------

select current_date() as current_date

-- COMMAND ----------

select current_timestamp as current_timestamp

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Date Arithmetic
-- MAGIC date_add\
-- MAGIC date_sub\
-- MAGIC datediff\
-- MAGIC add_months

-- COMMAND ----------

select date_add(current_date, 90) as next_quarter

-- COMMAND ----------

select datediff(current_date, '2020-09-29') as days

-- COMMAND ----------

select date_trunc('HOUR',current_timestamp ) as hour_beginning, current_timestamp()

-- COMMAND ----------

select current_timestamp as current_timestamp,
date_format(now(), 'yyyy-MM-dd') as date_now,
date_format(now(), 'DDD') as day_of_the_year,
date_format(now(), 'dd MMMM, yyyy') as current_date

-- COMMAND ----------

select to_date('20240111', 'yyyyMMdd') as result

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Calendar Functions

-- COMMAND ----------

describe function day

-- COMMAND ----------

describe function dayofmonth

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Unix Timestamp
-- MAGIC unix epoch - from 1970 

-- COMMAND ----------

--get unix epoch of current timestamp
SELECT unix_timestamp() as unix_timestamp