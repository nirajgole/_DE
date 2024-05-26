-- Databricks notebook source
-- MAGIC %run "/Users/jelyfis@protonmail.ch/ELT Data Pipeline for Daily Product Revenue/01 Cleanup Database and Datasets" $bronze_base=ng_db/bronze $gold_base=ng_db/gold

-- COMMAND ----------

-- MAGIC %run "/Users/jelyfis@protonmail.ch/ELT Data Pipeline for Daily Product Revenue/02 File Format Converter" $ds=orders $src_base=ng_db $bronze_base=ng_db/bronze

-- COMMAND ----------

-- MAGIC %run "/Users/jelyfis@protonmail.ch/ELT Data Pipeline for Daily Product Revenue/02 File Format Converter" $ds=order_items $src_base=ng_db $bronze_base=ng_db/bronze

-- COMMAND ----------

-- MAGIC %run "/Users/jelyfis@protonmail.ch/ELT Data Pipeline for Daily Product Revenue/03 Create Spark SQL Tables" $table_name=orders $bronze_base=ng_db/bronze

-- COMMAND ----------

-- MAGIC %run "/Users/jelyfis@protonmail.ch/ELT Data Pipeline for Daily Product Revenue/03 Create Spark SQL Tables" $table_name=order_items $bronze_base=ng_db/bronze

-- COMMAND ----------

SHOW tables

-- COMMAND ----------

SELECT count(*) FROM orders

-- COMMAND ----------

SELECT count(*) FROM order_items

-- COMMAND ----------

-- MAGIC %run "/Users/jelyfis@protonmail.ch/ELT Data Pipeline for Daily Product Revenue/04 Daily Product Revenue" $gold_baser=ng_db/gold

-- COMMAND ----------

-- %py
-- dbutils.fs.rm('dbfs:/external', True)