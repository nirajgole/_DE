# Databricks notebook source
# MAGIC %fs ls

# COMMAND ----------

# MAGIC %fs ls dbfs:/databricks-datasets/

# COMMAND ----------

# MAGIC %fs ls dbfs:/databricks-datasets/flights/

# COMMAND ----------

# MAGIC %fs ls dbfs:/databricks-datasets/flights/departuredelays.csv

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM csv.`dbfs:/databricks-datasets/flights/departuredelays.csv`
# MAGIC HEADER

# COMMAND ----------

# MAGIC %sql
# MAGIC -- INSERT OVERWRITE DIRECTORY 'dbfs:/databricks-datasets/airlines'
# MAGIC -- USING PARQUET

# COMMAND ----------

help(spark.read.csv)

# COMMAND ----------

spark

# COMMAND ----------

flights = spark.read.csv('dbfs:/databricks-datasets/flights/departuredelays.csv', inferSchema=True, header=True)
flights.show()

# COMMAND ----------

flights.\
    write.\
        mode('overwrite').\
            parquet('dbfs:/databricks-datasets/flights')