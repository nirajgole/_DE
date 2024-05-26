# Databricks notebook source
# MAGIC %md
# MAGIC #### Load Data

# COMMAND ----------

spark

# COMMAND ----------

df = spark.read.csv('dbfs:/databricks-datasets/retail-org/customers/customers.csv', header=True)
# inferSchema=True
# df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Get documentation using `help` function

# COMMAND ----------

help(df.show)

# COMMAND ----------

# MAGIC %md
# MAGIC ### `select`

# COMMAND ----------

# Lazy evaluation
df.select('customer_id')

# COMMAND ----------

df.select('customer_id').show(5,truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC #### `drop`
# MAGIC use drop columns when you have largenumber of columns and need to drop few columns\
# MAGIC or if you need few columns only then use `select` instead of drop and create new df

# COMMAND ----------

df.drop('state','city','postcode','street','region','district','lat','lon','ship_to_address').show(5)

# COMMAND ----------

help(df.collect)

# COMMAND ----------

help(f.to_date)

# COMMAND ----------

# MAGIC %md
# MAGIC #### `from_unixtime` - use this for epoch timestamp

# COMMAND ----------

df.select(f.from_unixtime('valid_from','yyyy-MM-dd').alias('date')).show()

# COMMAND ----------

from pyspark.sql import functions as f
from pyspark.sql import types as t
df.select('customer_id',
          f.from_unixtime('valid_to','yyyy-MM').alias('validity'))\
              .orderBy(f.col('validity').desc_nulls_last())\
              .show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC #### `withcolumn`

# COMMAND ----------

help(df.withColumn)

# COMMAND ----------

df.withColumn('valid_to',f.from_unixtime(f.col('valid_to'),'yyyy-MM'))\
        .withColumnRenamed('valid_to','validity')\
            .select('validity')\
                .orderBy(f.col('validity').desc_nulls_last())\
        .show()


# COMMAND ----------

# MAGIC %md
# MAGIC #### Write as Delta
# MAGIC Delta is nothing but parquet with additional copabilities.

# COMMAND ----------

df.write.format('delta').save('dbfs:/transformed/customer_delta')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Read Delta

# COMMAND ----------

# MAGIC %fs ls dbfs:/transformed/customer_delta

# COMMAND ----------

delta_df=spark.read.format('delta').load('dbfs:/transformed/customer_delta')


# COMMAND ----------

display(delta_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### `filter`

# COMMAND ----------

delta_df.select('customer_id',f.date_format(f.from_unixtime('valid_to'),'yyyy').alias('validity'))\
  .filter(f.col('validity')>2018).show(5)

# COMMAND ----------

delta_df.select('customer_id','tax_id').filter('tax_id is not null').show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC #### sum

# COMMAND ----------

# MAGIC %md
# MAGIC pyspark.sql.Column -> desc\
# MAGIC pyspark.sql.functions -> desc\
# MAGIC both works samilarly

# COMMAND ----------

delta_df.groupBy('loyalty_segment')\
  .agg(f.sum('units_purchased').alias('total_units_purchased'))\
          .orderBy((f.desc(f.col('total_units_purchased'))))\
      .show(5)
# .orderBy((f.col('total_units_purchased').desc()))\

# COMMAND ----------

# MAGIC %md
# MAGIC #### Sorting - based on one column
# MAGIC #### Composite Sorting - based on two or more columns

# COMMAND ----------

