-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### Spark SQL Ranking using Windowing

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/databricks-datasets/flights'

-- COMMAND ----------

select * from read_files(
  'dbfs:/databricks-datasets/flights/departuredelays.csv',
   format => 'csv',
  header => true,
  mode => 'FAILFAST'
)

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/databricks-datasets/nyctaxi/tripdata/yellow'

-- COMMAND ----------

create or replace temporary view yellow_taxi_trip_data
using csv
options (
  path='dbfs:/databricks-datasets/nyctaxi/tripdata/yellow/yellow_tripdata_2019-12.csv.gz',
  header=True
);
-- select * from csv.`dbfs:/databricks-datasets/airlines/`
select * from yellow_taxi_trip_data;

-- COMMAND ----------

desc table extended yellow_taxi_trip_data

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### RANK

-- COMMAND ----------

select
  rank() over (order by trip_distance desc) as rnk,
  dense_rank() over (order by trip_distance desc) as drnk,
  *
from yellow_taxi_trip_data
where passenger_count = 1
order by rnk


-- COMMAND ----------

select
row_number() over (partition by DOLocationID order by trip_distance desc) as rn,
dense_rank() over (partition by DOLocationID order by trip_distance desc) as drnk,
DOLocationID,
trip_distance,
  *
from yellow_taxi_trip_data
where passenger_count = 1