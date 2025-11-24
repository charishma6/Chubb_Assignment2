-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### displaying tables

-- COMMAND ----------

--bronze table
--view of taxi_raw_records table
select * from taxi_raw_records limit 10;

-- COMMAND ----------

--silver table
--view of flagged_rides table 
select * from flagged_rides limit 10;

-- COMMAND ----------

--silver table
--view of weekly_stats table 
select * from weekly_stats limit 10;

-- COMMAND ----------

--gold table
--view of top_n table
select * from top_n;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Validations

-- COMMAND ----------

-- MAGIC %md
-- MAGIC bronze layer validation

-- COMMAND ----------

--verifying that there are no records with non-positive trip_distance.
select count(*) as invalid_rows from taxi_raw_records where trip_distance <= 0;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC silver layer validation
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC - flagged_rides table

-- COMMAND ----------

--validating if there are only suspicious records in the flagged_rides table

select *
from flagged_rides f
Where not exists (
    select 1
    from taxi_raw_records r
    where f.week = date_trunc("week", r.tpep_pickup_datetime)
      and f.zip  = r.pickup_zip
      and (
           (r.pickup_zip = r.dropoff_zip and r.fare_amount > 50)
        or (r.trip_distance < 5 and r.fare_amount > 50)
      )
);


--it returns zero rows proving that there are no non-suspicious records in the flagged_rides table.

-- COMMAND ----------

--checking if there are any missing suspicious records 
--the records that are suspicious but not present in flagged_rides table.

select count(*) as missing_records from taxi_raw_records r
where (
    (r.pickup_zip = r.dropoff_zip AND r.fare_amount > 50) OR (r.trip_distance < 5 AND r.fare_amount > 50))
and not exists(
  select 1
  from flagged_rides f
  where f.week = date_trunc("week", r.tpep_pickup_datetime)
    and f.zip  = r.pickup_zip
  );

-- COMMAND ----------

-- MAGIC %md
-- MAGIC - weekly_stats table

-- COMMAND ----------

--checking if there any records with null values in the week column of the weekly_stats table
select *
from weekly_stats
WHERE week IS NULL;

-- COMMAND ----------

--verifying values of silver table by recomputing them from bronze data
Select
  ws.week,
  ws.avg_amount,
  ws.avg_distance,
  AVG(r.fare_amount)    as recomputed_avg_amount,
  AVG(r.trip_distance)  as recomputed_avg_distance
From weekly_stats ws
JOIN taxi_raw_records r
  on ws.week = date_trunc('week', r.tpep_pickup_datetime)
Group by ws.week, ws.avg_amount, ws.avg_distance
having
     ABS(AVG(r.fare_amount)   - ws.avg_amount)    > 0.0001
  OR ABS(AVG(r.trip_distance) - ws.avg_distance)  > 0.0001;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC gold layer validations

-- COMMAND ----------

--gold table should only have top-3 highest fare rides
select * from top_n;
--the output should only have 3 records

-- COMMAND ----------

--counting the rows of top_n table, it should return 3.
select count(*) no_of_records from top_n ;