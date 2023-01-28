
--Example
select lpep_pickup_datetime,
		lpep_dropoff_datetime,
		total_amount,
		concat(zp."Borough", '/', zp."Zone") as pickup_loc,
		concat(zd."Borough", '/', zd."Zone") as dropofff_loc
from yellow_taxi_trips as tr
inner join taxi_zones as zd on zd."LocationID" = tr."DOLocationID"
inner join taxi_zones as zp on zp."LocationID" = tr."PULocationID"
limit 10;
--630918

https://docs.google.com/forms/d/e/1FAIpQLSfZSkhUFQOf8Novq0aWTVY9LC0bJ1zlFOKiC-aVLwM-8LdxSg/viewform


--QUESTION 1
----iidfile string


--Q 2
--almazini@AMs-MacBook-Pro 1_docker_sql % docker run -it --entrypoint bash python:3.9
--root@70196ec1c2a0:/# pip list
--Package    Version
------------ -------
--pip        22.0.4
--setuptools 58.1.0
--wheel      0.38.4
--
--

--Q3
select count(1)
from yellow_taxi_trips as tr
where date(lpep_pickup_datetime) = date '2019-01-15'		--20689
	and date(lpep_dropoff_datetime) = date '2019-01-15'	;	--20530



--Q4 Which was the day with the largest trip distance Use the pick up time for your calculations.
select date(lpep_pickup_datetime) as dt, max(trip_distance) as max_dist
from yellow_taxi_trips as tr
group by 1
order by 2 desc
limit 1;
-- 2019-01-15


--Q5 In 2019-01-01 how many trips had 2 and 3 passengers?
select passenger_count, count(1)
from yellow_taxi_trips as tr
where date(lpep_pickup_datetime) = date '2019-01-01'
	and passenger_count in (2,3)
group by 1;
-- 2	1282
-- 3	254


-- Q 6. Largest tip
-- For the passengers picked up in the Astoria Zone which was the drop off zone that had the largest tip?
-- We want the name of the zone, not the id.
-- Note: it's not a typo, it's tip , not trip


select zd."Zone",  max(tr."tip_amount") as max_trip_distance
from yellow_taxi_trips as tr
inner join taxi_zones as zp on zp."LocationID" = tr."PULocationID" and zp."Zone" = 'Astoria'
inner join taxi_zones as zd on zd."LocationID" = tr."DOLocationID"
group by 1
order by 2 desc
limit 1;
-- Long Island City/Queens Plaza  88


