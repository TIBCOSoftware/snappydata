elapsedtime on;

---- Find Flights to SFO that has been delayed(Arrival/Dep) at least 5 times ----
SELECT FlightNum, COUNT(ArrDelay) as ARR_DEL
  FROM airline
  WHERE CAST(Dest AS VARCHAR(24)) = 'SFO'
      AND ArrDelay > 0
  GROUP BY FlightNum
  HAVING COUNT(ArrDelay) >= 5;

---- Query to get Avg ARR_DELAY with Airline name from AIRLINE and AIRLINE_REF table.----
SELECT AVG(ArrDelay) as AVGDELAY, count(*) as TOTALCNT, UniqueCarrier, airlineref.DESCRIPTION, Year_, Month_
  FROM airline, airlineref 
  WHERE airline.UniqueCarrier = airlineref.CODE 
  GROUP BY UniqueCarrier, DESCRIPTION, Year_,Month_;

---- List the flights and its details ,that are affected due to delays caused by weather ----
SELECT FlightNum, airlineref.DESCRIPTION 
  FROM airline, airlineref 
  WHERE airline.UniqueCarrier = airlineref.CODE AND  Origin like '%SFO%' AND WeatherDelay > 0 
  GROUP BY FlightNum ,DESCRIPTION;
