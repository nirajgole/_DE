-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### User Analysis

-- COMMAND ----------

DROP TABLE IF EXISTS users

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS users (
  user_id INT,
  user_first_name VARCHAR(50) NOT NULL,
  user_last_name VARCHAR(50) NOT NULL,
  email_id VARCHAR(255) NOT NULL,
  gender VARCHAR(10),
  phone_no VARCHAR(20),
  dob DATE,
  created_ts TIMESTAMP
)


-- COMMAND ----------

show tables

-- COMMAND ----------

select * from mock_data_csv

-- COMMAND ----------

select 
  year(created) as created_year,
  count(1) as user_count
from mock_data_csv
group by year(created)
order by user_count desc

-- COMMAND ----------

-- get the day name of the birth days for all the users born in the month of may
select 
  id, 
  first_name, 
  dob, 
  date_format(dob, 'EEEE') day_of_birth
from mock_data_csv
where month(dob) = 5
order by dayofweek(dob)

-- COMMAND ----------

-- get the names and email ids of users added in  year 2019
select 
  upper(concat_ws(' ',first_name, last_name)) as user_name,
  email as user_email
from mock_data_csv
where year(created) = 2019
order by id


-- COMMAND ----------

-- get the number of users by gender
select 
  case
    when gender = 'Male' then 'M'
    when gender = 'Female' then 'F'
    when gender is null then 'Not Specified'
    else 'Other'
  end as user_gender,
  count(1) as user_count
from mock_data_csv
group by user_gender
order by user_count desc

-- COMMAND ----------

desc function substr

-- COMMAND ----------

-- get first 4 digts of phone numbers of users having .gov domain in mail

select 
  id,
  first_name,
  email,
  substr(phone_no,1,4) as extension
from mock_data_csv
where email ilike '%gov%'


-- COMMAND ----------

show functions

-- COMMAND ----------

desc table mock_data_csv

-- COMMAND ----------

desc table extended mock_data_csv

-- COMMAND ----------

