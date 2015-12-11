----- CREATE TEMP TABLE -----
  CREATE TABLE IF NOT EXISTS AIRLINE_PARQUET_SOURCE
 (
  YearI INTEGER,
  MonthI INTEGER,
  DayOfMonth INTEGER,
  DayOfWeek INTEGER,
  DepTime INTEGER,
  CRSDepTime INTEGER,
  ArrTime INTEGER,
  CRSArrTime INTEGER,
  UniqueCarrier VARCHAR(24),
  FlightNum INTEGER,
  TailNum VARCHAR(25),
  ActualElapsedTime INTEGER,
  CRSElapsedTime INTEGER,
  AirTime INTEGER,
  ArrDelay INTEGER,
  DepDelay INTEGER,
  Origin VARCHAR(24),
  Dest VARCHAR(24),
  Distance INTEGER,
  TaxiIn INTEGER,
  TaxiOut INTEGER,
  Cancelled INTEGER,
  CancellationCode VARCHAR(24),
  Diverted INTEGER,
  CarrierDelay INTEGER,
  WeatherDelay INTEGER,
  NASDelay INTEGER,
  SecurityDelay INTEGER,
  LateAircraftDelay INTEGER,
  ArrDelaySlot INTEGER) 
  USING parquet OPTIONS(path '../../quickstart/data/airlineParquetData');

----- CREATE COLUMN TABLE -----  

CREATE TABLE IF NOT EXISTS AIRLINE USING column OPTIONS() AS (SELECT * FROM AIRLINE_PARQUET_SOURCE);
