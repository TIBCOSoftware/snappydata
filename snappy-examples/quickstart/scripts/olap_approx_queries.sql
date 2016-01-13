elapsedtime on;

-------------------------------------------------------------
---- Which airline had the most flights each year? ----
-------------------------------------------------------------
select  count(*) flightRecCount, description AirlineName, UniqueCarrier carrierCode ,Year_
   from airline , airlineref
   where airline.UniqueCarrier = airlineref.code
   group by UniqueCarrier,description, Year_
   order by flightRecCount desc limit 10
   with error 0.07 confidence 0.82;

-------------------------------------------------------------
---- Which Airlines Arrive On Schedule? ----
-------------------------------------------------------------
select AVG(ArrDelay) arrivalDelay, absolute_error(arrivalDelay) abs_err, relative_error(arrivalDelay) rel_err, UniqueCarrier carrier from airline
   group by UniqueCarrier
   order by arrivalDelay
   with error 0.07 confidence 0.95;

-------------------------------------------------------------
---- Which Airlines Arrive On Schedule? JOIN with reference table ----
-------------------------------------------------------------
select AVG(ArrDelay) arrivalDelay, absolute_error(arrivalDelay) abs_err, relative_error(arrivalDelay) rel_err, description AirlineName, UniqueCarrier carrier
  from airline, airlineref
  where airline.UniqueCarrier = airlineref.Code
  group by UniqueCarrier, description
  order by arrivalDelay
  with error 0.07 confidence 0.90;

-------------------------------------------------------------
---- What is the trend in arrival delays across all airlines in the US? ----
-------------------------------------------------------------
select AVG(ArrDelay) ArrivalDelay, absolute_error(arrivalDelay) abs_err, relative_error(arrivalDelay) rel_err, Year_
  from airline
  group by Year_
  order by Year_
  with error 0.17 confidence 0.87;

-------------------------------------------------------------
---- Which airline out of SanFrancisco had most delays due to weather ----
-------------------------------------------------------------
SELECT sum(WeatherDelay) totalWeatherDelay, lower_bound(totalWeatherDelay), upper_bound(totalWeatherDelay), absolute_error(totalWeatherDelay) abs_err, relative_error(totalWeatherDelay) rel_err, airlineref.DESCRIPTION
  FROM airline, airlineref
  WHERE airline.UniqueCarrier = airlineref.CODE AND  Origin like '%SFO%' AND WeatherDelay > 0
  GROUP BY DESCRIPTION
  limit 20
  with error 0.13 confidence 0.85;
