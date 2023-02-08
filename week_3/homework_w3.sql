CREATE OR REPLACE EXTERNAL TABLE `datatalks-data-course.dezoomcamp.fhv_tripdata_2019_raw_external`
OPTIONS (
  format = 'CSV',
  uris = ['gs://dtc_data_lake_datatalks-data-course/data/fhv_compressed/fhv_tripdata_2019-*.csv.gz']
);

CREATE OR REPLACE TABLE `datatalks-data-course.dezoomcamp.fhv_tripdata_2019_raw_bgq` AS
(
  SELECT * FROM `datatalks-data-course.dezoomcamp.fhv_tripdata_2019_raw_external`
);

-- Question 1:
-- What is the count for fhv vehicle records for year 2019?

SELECT count(1)
FROM `datatalks-data-course.dezoomcamp.fhv_tripdata_2019_raw_external`;
--43,244,696



-- Question 2:
-- Write a query to count the distinct number of affiliated_base_number for the entire dataset on both the tables.
-- What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?

SELECT count(distinct affiliated_base_number) as answ
FROM `datatalks-data-course.dezoomcamp.fhv_tripdata_2019_raw_external`;

SELECT count(distinct affiliated_base_number) as answ
FROM `datatalks-data-course.dezoomcamp.fhv_tripdata_2019_raw_bgq`;
-- 0 MB for the External Table and 317.94MB for the BQ Table


-- Question 3:
-- How many records have both a blank (null) PUlocationID and DOlocationID in the entire dataset?

SELECT count(1) as answ
FROM `datatalks-data-course.dezoomcamp.fhv_tripdata_2019_raw_external`
WHERE PUlocationID is null and DOlocationID is null;
--717,748


-- Question 4:
-- What is the best strategy to optimize the table if query always filter by pickup_datetime and order by affiliated_base_number?

--!! Partition by pickup_datetime Cluster on affiliated_base_number

CREATE OR REPLACE TABLE `datatalks-data-course.dezoomcamp.fhv_tripdata_2019_partitioned`
PARTITION BY DATE(pickup_datetime)
CLUSTER BY affiliated_base_number AS (
  SELECT *
  FROM `datatalks-data-course.dezoomcamp.fhv_tripdata_2019_raw_external`
);

-- Question 5:
-- Implement the optimized solution you chose for question 4.
-- Write a query to retrieve the distinct affiliated_base_number between pickup_datetime 03/01/2019 and 03/31/2019 (inclusive).
-- Use the BQ table you created earlier in your from clause and note the estimated bytes.
-- Now change the table in the from clause to the partitioned table you created for question 4 and note the estimated bytes processed. What are these values? Choose the answer which most closely matches.

SELECT count(distinct affiliated_base_number) as answ
from `datatalks-data-course.dezoomcamp.fhv_tripdata_2019_partitioned`
WHERE date(pickup_datetime) >= '2019-03-01' AND date(pickup_datetime) < '2019-04-01';
--23Mb

SELECT count(distinct affiliated_base_number) as answ
from `datatalks-data-course.dezoomcamp.fhv_tripdata_2019_raw_bgq`
where date(dropoff_datetime) BETWEEN '2019-03-01' AND '2019-03-31';
--647Mb

-- 647.87 MB for non-partitioned table and 23.06 MB for the partitioned table

-- Question 6:
-- Where is the data stored in the External Table you created?

--! GCP Bucket


-- Question 7:
-- It is best practice in Big Query to always cluster your data:

--False


-- (Not required) Question 8:
-- A better format to store these files may be parquet.
-- Create a data pipeline to download the gzip files and convert them into parquet. Upload the files to your GCP Bucket and create an External and BQ Table.

-- Note: Column types for all files used in an External Table must have the same datatype. While an External Table may be created and shown in the side panel in Big Query, this will need to be validated by running a count query on the External Table to check if any errors occur.

CREATE OR REPLACE EXTERNAL TABLE `datatalks-data-course.dezoomcamp.fhv_tripdata_2019_raw_external_parquet`
OPTIONS (
 format = 'parquet',
  uris = ['gs://dtc_data_lake_datatalks-data-course/data/fhv/fhv_tripdata_2019-*.parquet']
);


CREATE OR REPLACE TABLE `datatalks-data-course.dezoomcamp.fhv_tripdata_2019_raw_bgq_from_prqt` AS
(
  SELECT
    dispatching_base_num	--STRING	NULLABLE
    ,pickup_datetime	--STRING	NULLABLE
    ,dropOff_datetime	--STRING	NULLABLE
    ,SAFE_CAST(PUlocationID as INT64) as PUlocationID	--FLOAT	NULLABLE
    ,SAFE_CAST(DOlocationID as INT64) as DOlocationID	--FLOAT	NULLABLE
    ,SR_Flag	--FLOAT	NULLABLE
    ,Affiliated_base_number	--STRING	NULLABLE
  FROM `datatalks-data-course.dezoomcamp.fhv_tripdata_2019_raw_external_parquet`
WHERE  pickup_datetime < '2019-06-01'
UNION ALL
  SELECT
    dispatching_base_num	--STRING	NULLABLE
    ,pickup_datetime	--STRING	NULLABLE
    ,dropOff_datetime	--STRING	NULLABLE
    ,PUlocationID --FLOAT	NULLABLE
    ,DOlocationID	--FLOAT	NULLABLE
    ,SR_Flag	--FLOAT	NULLABLE
    ,Affiliated_base_number	--STRING	NULLABLE
  FROM `datatalks-data-course.dezoomcamp.fhv_tripdata_2019_raw_external_parquet`
WHERE  pickup_datetime > '2019-06-01'
);

