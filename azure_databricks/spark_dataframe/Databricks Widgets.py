# Databricks notebook source
# MAGIC %md
# MAGIC #### Widget to get job argument for script/notbook

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

dbutils.widgets.text('arg','Enter value here')

# COMMAND ----------

arg = dbutils.widgets.get('arg')

# COMMAND ----------

print(f'This is the argument passed via dbutils.widgets is : {arg}')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Argument in SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC select 'Selected argument is:  ${arg}'