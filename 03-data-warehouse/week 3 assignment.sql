-- Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `dlt-bigquery-484316.wk1_tf_dataset.2024_yellow_nyc_taxi_external`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://wk1-tf-bucket/yellow/year=2024/*']
);

-- Create a non partitioned table from external table
CREATE OR REPLACE TABLE dlt-bigquery-484316.wk1_tf_dataset.2024_yellow_nyc_taxi_non_partitioned AS
SELECT * FROM dlt-bigquery-484316.wk1_tf_dataset.2024_yellow_nyc_taxi_external;

-- Create a partitioned table from external table
CREATE OR REPLACE TABLE dlt-bigquery-484316.wk1_tf_dataset.2024_yellow_nyc_taxi_partitioned
PARTITION BY
  DATE(tpep_pickup_datetime) AS
SELECT * FROM dlt-bigquery-484316.wk1_tf_dataset.2024_yellow_nyc_taxi_external;

-- Creating a partition and cluster table
CREATE OR REPLACE TABLE dlt-bigquery-484316.wk1_tf_dataset.2024_yellow_nyc_taxi_partitioned_clustered
PARTITION BY DATE(tpep_pickup_datetime)
CLUSTER BY VendorID AS
SELECT * FROM dlt-bigquery-484316.wk1_tf_dataset.2024_yellow_nyc_taxi_external;




-- Q1 What is count of records for the 2024 Yellow Taxi Data?
SELECT COUNT(*)
FROM `dlt-bigquery-484316.wk1_tf_dataset.2024_yellow_nyc_taxi_external`;

--ANS: 20332093

-- Q2 Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables.
-- What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?

SELECT COUNT(DISTINCT PULocationID) FROM `dlt-bigquery-484316.wk1_tf_dataset.2024_yellow_nyc_taxi_external`;
--ANS 0Bytes

SELECT COUNT(DISTINCT PULocationID) FROM `dlt-bigquery-484316.wk1_tf_dataset.2024_yellow_nyc_taxi_non_partitioned`;
--ANS 155.12MB

--ANS 0 MB for the External Table and 155.12 MB for the Materialized Table


-- Q3 Write a query to retrieve the PULocationID from the table (not the external table) in BigQuery. Now write a query to retrieve the PULocationID and DOLocationID on the same table. Why are the estimated number of Bytes different?
SELECT PULocationID FROM `dlt-bigquery-484316.wk1_tf_dataset.2024_yellow_nyc_taxi_non_partitioned`;

--ANS 155.12BM

SELECT PULocationID, DOLocationID FROM `dlt-bigquery-484316.wk1_tf_dataset.2024_yellow_nyc_taxi_non_partitioned`;

--ANS 310.24BM

--ANS BigQuery is a columnar database, and it only scans the specific columns requested in the query. Querying two columns (PULocationID, DOLocationID) requires reading more data than querying one column (PULocationID), leading to a higher estimated number of bytes processed.


--Q4 How many records have a fare_amount of 0?
SELECT COUNT(*) FROM `dlt-bigquery-484316.wk1_tf_dataset.2024_yellow_nyc_taxi_external` WHERE fare_amount = 0;
--ANS 8333

--Q5 What is the best strategy to make an optimized table in Big Query if your query will always filter based on tpep_dropoff_datetime and order the results by VendorID (Create a new table with this strategy)

--ANS Partition by tpep_dropoff_datetime and Cluster on VendorID

--Q6 Write a query to retrieve the distinct VendorIDs between tpep_dropoff_datetime 2024-03-01 and 2024-03-15 (inclusive) 
--Use the materialized table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the partitioned table you created for question 5 and note the estimated bytes processed. What are these values?

SELECT DISTINCT(VendorID)
FROM dlt-bigquery-484316.wk1_tf_dataset.2024_yellow_nyc_taxi_non_partitioned
WHERE DATE(tpep_pickup_datetime) BETWEEN '2024-03-01' and '2024-03-15';

--ANS 310.24MB

SELECT DISTINCT(VendorID)
FROM dlt-bigquery-484316.wk1_tf_dataset.2024_yellow_nyc_taxi_partitioned
WHERE DATE(tpep_pickup_datetime) BETWEEN '2024-03-01' and '2024-03-15';

--ANS 26.85MB

--Q7 Where is the data stored in the External Table you created?

--ANS GCP Bucket


--Q8 It is best practice in Big Query to always cluster your data:

--ANS False