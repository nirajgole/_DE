-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS ng_db_bronze

-- COMMAND ----------

USE ng_db_bronze

-- COMMAND ----------

CREATE EXTERNAL TABLE IF NOT EXISTS ${table_name}
USING PARQUET
LOCATION 'dbfs:/external/ng_db/${table_name}'
AS SELECT * FROM PARQUET.`dbfs:/${bronze_base}/${table_name}`

-- OPTIONS (
--     path='${bronze_base}/${table_name}'
-- )


-- COMMAND ----------

select * from parquet.`dbfs:/ng_db/bronze/orders`