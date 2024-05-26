# Databricks notebook source
# MAGIC %fs ls dbfs:/

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

dbutils.widgets.text('src_base', '', label='Enter Source')
dbutils.widgets.text('bronze_base', '', label='Enter Target')
dbutils.widgets.text('ds', '', label='Enter Dataset Name')

# COMMAND ----------

src_base = dbutils.widgets.get('src_base')

# COMMAND ----------

bronze_base = dbutils.widgets.get('bronze_base')

# COMMAND ----------

ds = dbutils.widgets.get('ds')

# COMMAND ----------

# import json

# def get_columns(schemas_file, ds_name):
#     schema_text = spark.read.text(schemas_file, wholetext=True).first().value
#     schemas = json.loads(schema_text)
#     column_details = schemas[ds_name]
#     columns = [col['column_name'] for col in sorted(column_details, key=lambda col: col['column_position'])]
#     return columns

# COMMAND ----------

ds

# COMMAND ----------

# print(f'Processing {ds} data')
# columns = get_columns(f'dbfs:{src_base_dir}/schemas.json', ds)
# df = spark. \
#     read. \
#     csv(f'{src_base_dir}/{ds}', inferSchema=True). \
#     toDF(*columns)

# COMMAND ----------

df = spark.read.table(f'{src_base}.{ds}')

# COMMAND ----------

df.show()

# COMMAND ----------

df.write. \
    mode('overwrite'). \
    parquet(f'{bronze_base}/{ds}')

# COMMAND ----------

