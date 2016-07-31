-- DROP TABLE IF ALREADY EXISTS --
DROP TABLE IF EXISTS STAGING_AIRLINE ;
DROP TABLE IF EXISTS AIRLINE ;

----- CREATE TEMPORARY STAGING TABLE TO LOAD PARQUET FORMATTED DATA -----
CREATE EXTERNAL TABLE STAGING_AIRLINE
  USING parquet OPTIONS(path '../../quickstart/data/airlineParquetData');
-- Use below line to work with larger data set
--  USING parquet OPTIONS(path '../../quickstart/data/airlineParquetData_2007-15');

----- CREATE COLUMN TABLE -----  
----- Airline row count is small so we use 11 buckets (i.e. 11 partitions) -----
CREATE TABLE AIRLINE USING column OPTIONS(buckets '11') AS (
  SELECT Year AS Year_, Month AS Month_ , DayOfMonth,
    DayOfWeek, DepTime, CRSDepTime, ArrTime, CRSArrTime,
    UniqueCarrier, FlightNum, TailNum, ActualElapsedTime,
    CRSElapsedTime, AirTime, ArrDelay, DepDelay, Origin,
    Dest, Distance, TaxiIn, TaxiOut, Cancelled, CancellationCode,
    Diverted, CarrierDelay, WeatherDelay, NASDelay, SecurityDelay,
    LateAircraftDelay, ArrDelaySlot
    FROM STAGING_AIRLINE);
