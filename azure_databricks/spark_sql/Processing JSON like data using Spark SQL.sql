-- Databricks notebook source
show tables in default

-- COMMAND ----------

DROP TABLE IF EXISTS default.users

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.rm('dbfs:/user/hive/warehouse/users', True)

-- COMMAND ----------

CREATE OR REPLACE TABLE users(
  user_id INT,
  user_fname STRING,
  user_lname STRING,
  user_phones ARRAY<STRING>
) USING DELTA 
  LOCATION 'dbfs:/user/hive/warehouse/users'

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/user/hive/warehouse/

-- COMMAND ----------

INSERT INTO
  users
VALUES
  (1, 'John', 'Jacobs', ARRAY('9874563210','9856321470')),
  (2, 'James', 'Gordon', ARRAY('')),
  (3, 'John', 'Jacobs', NULL)

-- COMMAND ----------

SELECT * FROM USERS

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Explode Outer

-- COMMAND ----------

select *,
  explode_outer(user_phones) as user_phone
from users

-- COMMAND ----------

select user_id, len(user_fname) as len_fname, size(user_phones) as len_phones
from users

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### STRUCT

-- COMMAND ----------

DROP TABLE IF EXISTS default.users

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.rm('dbfs:/user/hive/warehouse/users', True)

-- COMMAND ----------

CREATE OR REPLACE TABLE users(
  user_id INT,
  user_fname STRING,
  user_lname STRING,
  user_phones STRUCT<home: STRING, mobile: STRING>
) USING DELTA 
  LOCATION 'dbfs:/user/hive/warehouse/users'

-- COMMAND ----------

INSERT INTO
  users
VALUES
  (1, 'John', 'Jacobs', STRUCT('9874563210','9856321470')),
  (2, 'James', 'Gordon', STRUCT(NULL,'')),
  (3, 'John', 'Jacobs', STRUCT('',''))

-- COMMAND ----------

select * from users

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### flattened structs

-- COMMAND ----------

select user_fname, user_phones.* from users

-- COMMAND ----------

select user_fname, user_phones.home, user_phones.mobile from users

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.rm('dbfs:/user/hive/warehouse/struct_users', True)

-- COMMAND ----------

CREATE  TABLE struct_users(
  user_id INT,
  user_name STRING,
  user_phones ARRAY<STRUCT<phone_type:STRING, phone_number:STRING>>
)

-- COMMAND ----------

insert into struct_users
values(1,'James Webb', ARRAY(STRUCT('HOME','98746632122'),STRUCT('OFFICE','4565465465465')))

-- COMMAND ----------

SELECT * FROM struct_users

-- COMMAND ----------

select *, explode_outer(user_phones) from struct_users

-- COMMAND ----------

with explodeed_cte as (
  select  *, explode_outer(user_phones) as user_phones_exploded from struct_users
)
select user_id, user_name, user_phones_exploded.* from explodeed_cte

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### split, concat_ws, collect_*, array_*, collect_list

-- COMMAND ----------

create or replace table collect_users as
select user_id, user_name, user_phones_exploded.* from (select  *, explode_outer(user_phones) as user_phones_exploded from struct_users)

-- COMMAND ----------

select * from collect_users

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### collect_list, concat_ws

-- COMMAND ----------

select user_id, user_name, concat_ws(',',collect_list(phone_number)) as user_phone_numbers
from collect_users
group by user_id, user_name

-- COMMAND ----------

select
  user_id,
  user_name,
  struct(phone_type, phone_number) as phone_numbers_type
from collect_users

-- COMMAND ----------

select
  user_id,
  user_name,
  collect_list(nvl2(phone_number, struct(phone_type,phone_number), null)) as user_phones
  from collect_users
  group by   user_id, user_name

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Phone Numbers Extraction

-- COMMAND ----------

create table split_users(
  user_name string,
  user_phone string
)

-- COMMAND ----------

insert into split_users
values
('James','9874563210,456253251'),
('Brick','789652366,456253251')

-- COMMAND ----------

select user_name, explode_outer(split(user_phone,',')) as user_phone from split_users