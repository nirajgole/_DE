# Databricks notebook source
df = spark.read.csv('dbfs:/databricks-datasets/retail-org/customers/customers.csv', header=True)

# COMMAND ----------

df.show()

# COMMAND ----------

help(df.createOrReplaceTempView)

# COMMAND ----------

df.createOrReplaceTempView('t_customeer')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from t_customeer

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables

# COMMAND ----------

from pyspark.sql import functions as f

# COMMAND ----------

df_units_purchased = df\
    .groupBy('city')\
    .agg(f.sum(f.col('units_purchased')).alias('total_units_purchased'))\
    .orderBy(f.col('total_units_purchased').desc())\
    .select('city','total_units_purchased')\
    .where('city is not null and total_units_purchased > 1000')

df_units_purchased.show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Writte to Metastore
# MAGIC #### insert_into, saveAsTable
# MAGIC #### write operation ->
# MAGIC > append -> add to exsisting,\
# MAGIC > overwrite -> truncate existing and add new,\
# MAGIC > error, ignore 

# COMMAND ----------

# MAGIC %md
# MAGIC #### saveAsTable

# COMMAND ----------

# df_units_purchased.write.saveAsTable('total_units_purchased',format='parquet', mode='append')
df_units_purchased.write.saveAsTable('total_units_purchased',format='parquet', mode='overwrite')


# COMMAND ----------

# MAGIC %sql
# MAGIC show tables

# COMMAND ----------

# MAGIC %sql
# MAGIC -- truncate table total_units_purchased;
# MAGIC select * from total_units_purchased;

# COMMAND ----------

# MAGIC %md
# MAGIC #### insertInto

# COMMAND ----------

help(df_units_purchased.write.insertInto)

# COMMAND ----------

df_units_purchased.write.insertInto('total_units_purchased', overwrite=False)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from total_units_purchased