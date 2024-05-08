-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### SQL Word-Count

-- COMMAND ----------

DROP TABLE IF EXISTS lines

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS lines (s STRING)

-- COMMAND ----------

INSERT INTO lines VALUES
('Hello World!.'),
('How are you?'),
('All Good'),
('Nothing much'),
('Let me tell you one thing, brick by brick databricks'),
('Ha ha ha ha'),
('Nice to meet you.'),
('Thanks, have a nice day.')


-- COMMAND ----------

select * from lines

-- COMMAND ----------

select split(s,' ') as word_array from lines

-- COMMAND ----------

describe function explode

-- COMMAND ----------

select explode(split(s,' ')) as words from lines

-- COMMAND ----------

-- happy solution
with cte (
select explode(split(s,' ')) as words from lines)

select 
  words, 
  count(1) as count 
from cte
group by words
order by count

-- COMMAND ----------

describe function regexp_replace

-- COMMAND ----------

SELECT regexp_replace('100-200', '(\\d+)', 'num');


-- COMMAND ----------

-- final solution
with cte (
  select
    explode(
      split(
        regexp_replace(lower(s), '[^a-z\\s]', ''),
        ' '
      )
    ) as words
  from
    lines
)
select
  words,
  count(1) as count
from
  cte
group by
  words
order by
  count desc