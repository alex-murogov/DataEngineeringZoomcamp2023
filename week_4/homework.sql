CREATE OR REPLACE EXTERNAL TABLE `datatalks-data-course.trips_data_all.green_tripdata`
OPTIONS ( format = 'PARQUET',
    uris = ['gs://dtc_data_lake_datatalks-data-course/data/green/green_tripdata_2019-*.parquet',
    'gs://dtc_data_lake_datatalks-data-course/data/green/green_tripdata_2020-*.parquet'] );


CREATE OR REPLACE EXTERNAL TABLE `datatalks-data-course.trips_data_all.yellow_tripdata`
  OPTIONS ( format = 'PARQUET',
    uris = ['gs://dtc_data_lake_datatalks-data-course/data/yellow/yellow_tripdata_2019-*.parquet',
        'gs://dtc_data_lake_datatalks-data-course/data/yellow/yellow_tripdata_2020-*.parquet'] );


CREATE OR REPLACE EXTERNAL TABLE `datatalks-data-course.trips_data_all.fhv_2019`
OPTIONS (
   format = 'PARQUET',
    uris = ['gs://dtc_data_lake_datatalks-data-course/data/fhv/fhv_tripdata_2019-*.parquet'] );


-- Question 1:
SELECT count(1) FROM `datatalks-data-course.dbt_alexmurogov.fact_trips`;
--61604284


-- Question 2:
SELECT service_type, count(1) as cnt FROM `datatalks-data-course.dbt_alexmurogov.fact_trips` group by 1;

-- Yellow 55349400
-- Green 6254884

select (55349400 *100.00/ (6254884+ 55349400)) as yellow, (6254884 *100.00/ (6254884+ 55349400)) as grreen;
--89.9/10.1
-- 89.84667365016368 / 10.15332634983632



-- Question 3:
SELECT count(1) FROM `datatalks-data-course.dbt_alexmurogov.stg_fhv_tripdata`;
--43244696


-- Question 4:
SELECT count(1) FROM `datatalks-data-course.dbt_alexmurogov.fact_fhv_trips`
where pickup_zone is not null and   dropoff_zone is not null
limit 10 ;
--23936461


-- Question 5:
SELECT EXTRACT(month from PARSE_DATETIME('%Y-%m-%d %H:%M:%S', pickup_datetime)) as Month, count(1) as rides_count
--  select * /
 FROM `datatalks-data-course.dbt_alexmurogov.fact_fhv_trips`
--  limit 10
 group by 1
 order by 2 desc
limit 3 ;

-- 1  20511716
-- 12 380031
-- 10 377929

