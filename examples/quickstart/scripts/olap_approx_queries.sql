elapsedtime on;
set spark.sql.shuffle.partitions=6;

-------------------------------------------------------------
---- Which airline had the most flights each year? ----
-------------------------------------------------------------
select  count(*) flightRecCount, description AirlineName, UniqueCarrier carrierCode ,Year_
   from airline , airlineref
   where airline.UniqueCarrier = airlineref.code
   group by UniqueCarrier,description, Year_
   order by flightRecCount desc limit 10
   with error 0.20 confidence 0.80;

-------------------------------------------------------------
---- Which Airlines Arrive On Schedule? ----
-------------------------------------------------------------
select AVG(ArrDelay) arrivalDelay, relative_error(arrivalDelay) rel_err,
   UniqueCarrier carrier from airline_sample
   group by UniqueCarrier
   order by arrivalDelay;

-------------------------------------------------------------
---- Which Airlines Arrive On Schedule? JOIN with reference table ----
-------------------------------------------------------------
select AVG(ArrDelay) arrivalDelay, relative_error(arrivalDelay) rel_err,
  description AirlineName, UniqueCarrier carrier
  from airline_sample, airlineref
  where airline_sample.UniqueCarrier = airlineref.Code
  group by UniqueCarrier, description
  order by arrivalDelay;

-------------------------------------------------------------
---- What is the trend in arrival delays across all airlines in the US? ----
-------------------------------------------------------------
select AVG(ArrDelay) ArrivalDelay, relative_error(arrivalDelay) rel_err, Year_
  from airline
  group by Year_
  order by Year_
  with error 0.20 confidence 0.80;
