# Databricks notebook source
# MAGIC %md
# MAGIC #### Spark Metastore

# COMMAND ----------

spark

# COMMAND ----------

help(spark.catalog)

# COMMAND ----------

spark.catalog.currentDatabase()

# COMMAND ----------

spark.catalog.listDatabases()

# COMMAND ----------

spark.catalog.listCatalogs()

# COMMAND ----------

for i in spark.catalog.listTables('ng_db'):
    print(i.name)