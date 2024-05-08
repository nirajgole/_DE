-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### Saving query result in spark metastore

-- COMMAND ----------

show tables

-- COMMAND ----------

desc table extended users_main_csv

-- COMMAND ----------

create table users_staging as 
select * from users_main_csv

-- COMMAND ----------

select * from users_staging

-- COMMAND ----------

select 
  id,
  first_name,
  email,
  substr(phone_no,1,4) as extension
from users_main_csv
where email ilike '%gov%'

-- COMMAND ----------

-- ctas
create table users_staging as 
select 
  id,
  first_name,
  email,
  substr(phone_no,1,4) as extension
from users_staging
where email ilike '%gov%'
-- solution to error
-- drop or replace existing table

-- COMMAND ----------

-- overwrite
insert overwrite users_staging
select 
  id,
  first_name,
  email,
  substr(phone_no,1,4) as extension
from users_staging
where email ilike '%gov%'
-- need same number of columns

-- COMMAND ----------

drop table if exists users_staging_dev;

create table users_staging_dev as 
select 
  id,
  first_name,
  email,
  substr(phone_no,1,4) as extension
from users_staging
where email ilike '%gov%';


-- COMMAND ----------

select * from users_staging_dev limit 10

-- COMMAND ----------

insert overwrite users_staging_dev
select 
  id,
  first_name,
  email,
  substr(phone_no,1,4) as extension
from users_staging
where email ilike '%gov%'

-- COMMAND ----------

select * from users_staging_dev limit 10

-- COMMAND ----------

merge into users_staging_dev as usd
using (
select 
  id,
  first_name,
  email,
  substr(phone_no,1,4) as extension
from users_staging
where email ilike '%gov%'
 ) as usdr
 ON usd.id = usdr.id
when matched then update set *
when not matched then insert *