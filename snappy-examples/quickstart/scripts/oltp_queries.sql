---- Tables in the DB are ----
show tables in APP;

---- Schema for column table ----
describe airline;

---- Column table has the following number of entries ----
select count(*) as TotalColTableEntries from airline;

---- Schema for Row table ----
describe airlineref;

---- Row table has the following number of entries ----
select count(*) as TotalRowTableEntries from airlineref;

---- Find Flights to SAN that has been delayed(Arrival/Dep)  5 or more times in a row ----
SELECT FlightNum,COUNT(ArrDelay) as ARR_DEL FROM airline WHERE Dest='SAN' group by FlightNum HAVING COUNT(ArrDelay) >= 5;

---- Query to get Averag ARR_DELAY with Description from AIRLINE and AIRLINE_REF table.----
SELECT AVG(ArrDelay) as AVGDELAY, count(*) as TOTALCNT, UniqueCarrier, t2.DESCRIPTION, Year, Month FROM airline t1, airlineref t2 where t1.UniqueCarrier = t2.CODE GROUP BY UniqueCarrier, DESCRIPTION, Year,Month;

---- List the flights and its details ,that are affected due to delays caused by weather in a particular state ----
SELECT FlightNum,t2.DESCRIPTION FROM airline t1, airlineref t2 where t1.UniqueCarrier = t2.CODE AND  Origin like '%SMF%' AND WeatherDelay > 0 group by FlightNum ,DESCRIPTION;
