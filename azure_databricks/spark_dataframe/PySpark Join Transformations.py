# Databricks notebook source
spark

# COMMAND ----------

# MAGIC %fs ls dbfs:/database/orders

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT current_catalog(), current_database(), current_schema();

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE movies
# MAGIC (
# MAGIC     id INT,
# MAGIC     genre STRING,
# MAGIC     title STRING
# MAGIC ) using delta

# COMMAND ----------

# MAGIC %sql
# MAGIC truncate table reviews

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO movies
# MAGIC VALUES
# MAGIC     (1, 'Action', 'The Dark Knight'),
# MAGIC     (2, 'Action', 'Avengers: Infinity War'),
# MAGIC     (3, 'Action', 'Gladiator'),
# MAGIC     (4, 'Action', 'Die Hard'),
# MAGIC     (5, 'Action', 'Mad Max: Fury Road')
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create reviews table
# MAGIC CREATE TABLE reviews
# MAGIC (
# MAGIC     movie_id INT,
# MAGIC     rating DECIMAL(3,1)
# MAGIC ) using delta;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO reviews
# MAGIC     (movie_id, rating)
# MAGIC VALUES
# MAGIC     (1, 4.5),
# MAGIC     (1, 4.0),
# MAGIC     (1, 5.0),
# MAGIC     (2, 4.2),
# MAGIC     (2, 4.8),
# MAGIC     (2, 3.9),
# MAGIC     (3, 4.6),
# MAGIC     (3, 3.8),
# MAGIC     (3, 4.3),
# MAGIC     (6, 4.8),
# MAGIC     (6, 4.7),
# MAGIC     (6, 4.9),
# MAGIC     (7, 4.6),
# MAGIC     (7, 4.9),
# MAGIC     (7, 4.3)

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC select * from movies

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from reviews

# COMMAND ----------

# MAGIC %fs ls dbfs:/user/hive/warehouse

# COMMAND ----------

movies_df = spark.read.table('movies')
display(movies_df)

# COMMAND ----------

reviews_df = spark.read.table('reviews')
display(reviews_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Inner Join

# COMMAND ----------

help(movies_df.join)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Inner

# COMMAND ----------

inner_join_df = movies_df.join(reviews_df, movies_df['id']==reviews_df['movie_id'], 'inner')
display(inner_join_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Left

# COMMAND ----------

left_join_df = movies_df.join(reviews_df, movies_df['id']==reviews_df['movie_id'], 'left')
display(left_join_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Right

# COMMAND ----------

right_join_df = movies_df.join(reviews_df, movies_df['id']==reviews_df['movie_id'], 'right')
display(left_join_df)

# COMMAND ----------

left_join_df.select(movies_df['*'], reviews_df['rating']).show(truncate=False)

# COMMAND ----------

from pyspark.sql import functions as f
avg_rating = left_join_df.groupBy('title')\
    .agg(f.avg('rating').alias('avg_rating'))\
        .orderBy(f.col('avg_rating').desc_nulls_last())

avg_rating.show()