-- DROP TABLE IF ALREADY EXISTS --
DROP TABLE IF EXISTS STAGING_AIRLINE ;
DROP TABLE IF EXISTS AIRLINE ;

----- CREATE PARQUET TABLE THAT IS USED TO LOAD THE DATA IN SNAPPY -----
CREATE TABLE STAGING_AIRLINE
  USING parquet OPTIONS(path '../../quickstart/data/airlineParquetData');

----- CREATE COLUMN TABLE -----  
CREATE TABLE AIRLINE USING column OPTIONS() AS (
  SELECT Year AS Year_, Month AS Month_ , DayOfMonth,
    DayOfWeek, DepTime, CRSDepTime, ArrTime, CRSArrTime,
    UniqueCarrier, FlightNum, TailNum, ActualElapsedTime,
    CRSElapsedTime, AirTime, ArrDelay, DepDelay, Origin,
    Dest, Distance, TaxiIn, TaxiOut, Cancelled, CancellationCode,
    Diverted, CarrierDelay, WeatherDelay, NASDelay, SecurityDelay,
    LateAircraftDelay, ArrDelaySlot
    FROM AIRLINE_PARQUET_SOURCE);
