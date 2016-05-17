elapsedtime on;
set spark.sql.shuffle.partitions=6;

-------------------------------------------------------------
---- Which airline had the most flights each year? ----
-------------------------------------------------------------
select  count(*) flightRecCount, description AirlineName, UniqueCarrier carrierCode ,Year_ 
   from airline , airlineref
   where airline.UniqueCarrier = airlineref.code
   group by UniqueCarrier,description, Year_ 
   order by flightRecCount desc limit 10 ;

-------------------------------------------------------------
---- Which Airlines Arrive On Schedule? ----
-------------------------------------------------------------
select AVG(ArrDelay) arrivalDelay, UniqueCarrier carrier from airline   
   group by UniqueCarrier
   order by arrivalDelay;

-------------------------------------------------------------
---- Which Airlines Arrive On Schedule? JOIN with reference table ----
-------------------------------------------------------------
select AVG(ArrDelay) arrivalDelay, description AirlineName, UniqueCarrier carrier 
  from airline, airlineref
  where airline.UniqueCarrier = airlineref.Code 
  group by UniqueCarrier, description 
  order by arrivalDelay;

-------------------------------------------------------------
---- What is the trend in arrival delays across all airlines in the US? ----
-------------------------------------------------------------
select AVG(ArrDelay) ArrivalDelay, Year_
  from airline 
  group by Year_ 
  order by Year_ ;

-------------------------------------------------------------
---- Which airline out of SanFrancisco had most delays due to weather ----
-------------------------------------------------------------
SELECT sum(WeatherDelay) totalWeatherDelay, airlineref.DESCRIPTION 
  FROM airline, airlineref 
  WHERE airline.UniqueCarrier = airlineref.CODE AND  Origin like '%SFO%' AND WeatherDelay > 0 
  GROUP BY DESCRIPTION 
  limit 20;
