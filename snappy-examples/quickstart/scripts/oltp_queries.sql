---- Find Flights to SAN that has been delayed(Arrival/Dep)  5 or more times in a row ----
SELECT FlightNum,COUNT(ArrDelay) as ARR_DEL
  FROM airline 
  WHERE CAST(Dest AS VARCHAR(24)) = 'SAN'
  GROUP BY FlightNum
  HAVING COUNT(ArrDelay) >= 5;

---- Query to get Averag ARR_DELAY with Description from AIRLINE and AIRLINE_REF table.----
SELECT AVG(ArrDelay) as AVGDELAY, count(*) as TOTALCNT, UniqueCarrier, airlineref.DESCRIPTION, Year, Month
  FROM airline, airlineref 
  WHERE airline.UniqueCarrier = airlineref.CODE 
  GROUP BY UniqueCarrier, DESCRIPTION, Year,Month;

---- List the flights and its details ,that are affected due to delays caused by weather in a particular state ----
SELECT FlightNum, airlineref.DESCRIPTION 
  FROM airline, airlineref 
  WHERE airline.UniqueCarrier = airlineref.CODE AND  Origin like '%SMF%' AND WeatherDelay > 0 
  GROUP BY FlightNum ,DESCRIPTION;
