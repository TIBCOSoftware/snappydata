-- DROP TABLE IF ALREADY EXISTS --
DROP TABLE IF EXISTS STAGING_AIRLINE ;
DROP TABLE IF EXISTS AIRLINE ;

----- CREATE TEMPORARY STAGING TABLE TO LOAD PARQUET FORMATTED DATA -----
CREATE EXTERNAL TABLE STAGING_AIRLINE
     -- USING parquet OPTIONS(path 's3a://AKIAILHSQ3FINHV473RQ:Tnn6GOHhjaayIRtVNApYQgNLU3FxXkw9albr9hVJ@zeppelindemo/airlinedata_cleaned');
      USING parquet OPTIONS(path '/export/shared/QA_DATA/airlinedata_cleaned');

----- CREATE COLUMN TABLE -----
CREATE TABLE AIRLINE USING column OPTIONS(partition_by 'id,UniqueCarrier,Year_', PERSISTENT 'SYNCHRONOUS', BUCKETS '384', EVICTION_BY 'LRUHEAPPERCENT', overflow 'true', key_columns 'id,UniqueCarrier,Year_')  AS (
  SELECT cast(monotonically_increasing_id() as string) as id, '' as dummy, Year_, Month_ , DayOfMonth, DepTime, CRSDepTime, ArrTime, UniqueCarrier, ArrDelay, DepDelay, Origin,
    Dest, Distance, TaxiIn, TaxiOut, Cancelled, CarrierDelay, WeatherDelay
    FROM STAGING_AIRLINE);

-- DROP TABLE IF ALREADY EXISTS --
DROP TABLE IF EXISTS STAGING_AIRLINEREF;
DROP TABLE IF EXISTS AIRLINEREF;

----- CREATE TEMPORARY STAGING TABLE TO LOAD PARQUET FORMATTED DATA -----
CREATE EXTERNAL TABLE STAGING_AIRLINEREF
     -- USING parquet OPTIONS(path 's3a://AKIAILHSQ3FINHV473RQ:Tnn6GOHhjaayIRtVNApYQgNLU3FxXkw9albr9hVJ@zeppelindemo/airportdata');
      USING parquet OPTIONS(path '/export/shared/QA_DATA/airportdata');

----- CREATE ROW TABLE -----

CREATE TABLE AIRLINEREF USING row OPTIONS(PERSISTENT 'SYNCHRONOUS') AS (SELECT CODE, DESCRIPTION FROM STAGING_AIRLINEREF);