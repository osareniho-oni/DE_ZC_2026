--SQL Queries for data in BigQuery

--Question 3. How many rows are there for the Yellow Taxi data for all CSV files in the year 2020?
SELECT COUNT(*) FROM `dlt-bigquery-484316.wk1_tf_dataset.yellow_tripdata` 
  WHERE TIMESTAMP_TRUNC(tpep_pickup_datetime, YEAR) >= TIMESTAMP '2020-01-01 00:00:00+00' 
  and TIMESTAMP_TRUNC(tpep_pickup_datetime, YEAR) < TIMESTAMP '2021-01-01 00:00:00+00'
---ANS 24,649,256

--Question 4. How many rows are there for the Green Taxi data for all CSV files in the year 2020?
SELECT COUNT(*) FROM `dlt-bigquery-484316.wk1_tf_dataset.green_tripdata` 
  WHERE TIMESTAMP_TRUNC(lpep_pickup_datetime, YEAR) >= TIMESTAMP '2020-01-01 00:00:00+00'
  and TIMESTAMP_TRUNC(lpep_pickup_datetime, YEAR) < TIMESTAMP '2021-01-01 00:00:00+00'
--ANS 1,734,164

--Question 5. How many rows are there for the Yellow Taxi data for the March 2021 CSV file?
SELECT COUNT(*)
FROM `dlt-bigquery-484316.wk1_tf_dataset.yellow_tripdata`
WHERE tpep_pickup_datetime >= TIMESTAMP '2021-03-01 00:00:00+00'
  AND tpep_pickup_datetime <  TIMESTAMP '2021-04-01 00:00:00+00';)
--ANS 1,925,130
