# Databricks notebook source
spark

# COMMAND ----------



# COMMAND ----------

# MAGIC %fs ls dbfs:/user/hive/warehouse/movies

# COMMAND ----------

# MAGIC %md
# MAGIC #### Global Rank - orderBy
# MAGIC #### Ranks with each Partition or group - partition by and orderBy

# COMMAND ----------

from pyspark.sql import functions as f
from pyspark.sql import types as t

# COMMAND ----------

df = spark.read.csv('dbfs:/databricks-datasets/retail-org/customers/customers.csv', header=True)
df.printSchema()

# COMMAND ----------

df.groupBy('state').agg(f.round(f.avg('units_purchased'),2).alias('Avg Units Purchased')).show()

# COMMAND ----------

from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC #### Dense Rank

# COMMAND ----------

df.withColumn('drnk',f.dense_rank().over(Window.orderBy(f.col('units_purchased').desc()))).where(f.col('drnk')<=5).select('state','units_purchased','drnk').show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Partition By

# COMMAND ----------

df.withColumn('drnk',f.dense_rank().over(Window.partitionBy('state').orderBy(f.col('units_purchased').desc()))).where(f.col('drnk')<=5).select('state','units_purchased','drnk').show()