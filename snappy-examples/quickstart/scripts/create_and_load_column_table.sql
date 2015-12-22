----- CREATE TEMP TABLE -----
CREATE TABLE IF NOT EXISTS AIRLINE_PARQUET_SOURCE
  USING parquet OPTIONS(path '../../quickstart/data/airlineParquetData');

----- CREATE COLUMN TABLE -----  
CREATE TABLE IF NOT EXISTS AIRLINE USING column OPTIONS() AS (
  SELECT Year AS Year_, Month AS Month_ , DayOfMonth,
    DayOfWeek, DepTime, CRSDepTime, ArrTime, CRSArrTime,
    UniqueCarrier, FlightNum, TailNum, ActualElapsedTime,
    CRSElapsedTime, AirTime, ArrDelay, DepDelay, Origin,
    Dest, Distance, TaxiIn, TaxiOut, Cancelled, CancellationCode,
    Diverted, CarrierDelay, WeatherDelay, NASDelay, SecurityDelay,
    LateAircraftDelay, ArrDelaySlot
    FROM AIRLINE_PARQUET_SOURCE);
