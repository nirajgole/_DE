-- Databricks notebook source
-- MAGIC %python
-- MAGIC spark

-- COMMAND ----------

-- MAGIC %fs ls

-- COMMAND ----------

DROP DATABASE IF EXISTS ng_db CASCADE

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS ng_db LOCATION 'dbfs:/database'

-- COMMAND ----------

describe database ng_db

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/database/orders

-- COMMAND ----------

USE ng_db

-- COMMAND ----------

SELECT current_database()

-- COMMAND ----------

CREATE TABLE ng_db.orders (
  order_id INT,
  order_date DATE,
  order_customer_id BIGINT,
  order_status STRING
) USING DELTA

-- COMMAND ----------

describe orders

-- COMMAND ----------

describe formatted orders

-- COMMAND ----------

COPY INTO orders
FROM 'dbfs:/'
FILEFORMAT = JSON

-- COMMAND ----------

-- creating table in spark metastore
CREATE EXTERNAL TABLE ng_db.order_items(
  id INT,
  price FLOAT
) USING DELTA
OPTIONS (
  path='dbfs:/external/'
)

-- COMMAND ----------

DESCRIBE FORMATTED ng_db.order_items

-- COMMAND ----------

MERGE INTO order_items as oi
USING orders as s o
  on oi.id = s.id
WHEN MATCHED THEN UPDATE SET
  oi.price = o.price
  oi.item = o.item
WHEN NOT MATCHED THEN INSERT
  (oi.price,oi.item)
VALUES
  (o.id, o.item)