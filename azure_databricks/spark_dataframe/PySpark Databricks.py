# Databricks notebook source
# %fs ls dbfs:/user/hive/warehouse/

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/tables/users.csv

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM read_files(
# MAGIC   'dbfs:/FileStore/tables/users.csv',
# MAGIC   format => 'csv',
# MAGIC   header => true,
# MAGIC   mode => 'FAILFAST')

# COMMAND ----------

# display(spark.read.csv('dbfs:/FileStore/tables/users.csv',inferSchema=True, header=True))
spark.read.csv('dbfs:/FileStore/tables/users.csv',inferSchema=True, header=True).show(truncate=False)

# COMMAND ----------

# MAGIC %fs ls dbfs:/databricks-datasets/retail-org/

# COMMAND ----------

# MAGIC %fs ls dbfs:/databricks-datasets/retail-org/customers

# COMMAND ----------

# spark.read.csv('dbfs:/databricks-datasets/retail-org/sales_orders', header=True).show(truncate=False)
# display(spark.read.csv('dbfs:/databricks-datasets/retail-org/customers/customers.csv', header=True))
df = spark.read.csv('dbfs:/databricks-datasets/retail-org/customers/customers.csv', header=True)
display(df)

# COMMAND ----------

from pyspark.sql import functions as f
from pyspark.sql.types import DoubleType

region_table = df.groupBy(f.col('region'))\
    .agg(f.sum(f.col('units_purchased').cast(DoubleType())).alias('Total_Region_Purchase'))\
        .orderBy(f.desc(f.col('Total_Region_Purchase')))\

region_table.show(5)

# COMMAND ----------

region_table.write\
    .mode('overwrite')\
        .parquet('dbfs:/transformed/customer_region_purchase/')


# COMMAND ----------

# MAGIC %fs ls dbfs:/transformed/customer_region_purchase/

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM parquet.`dbfs:/transformed/customer_region_purchase/`
# MAGIC

# COMMAND ----------

# row count
df.select('customer_id').distinct().count()