-- DROP TABLE IF ALREADY EXISTS --
DROP TABLE IF EXISTS STAGING_AIRLINE ;
DROP TABLE IF EXISTS AIRLINE ;

----- CREATE TEMPORARY STAGING TABLE TO LOAD PARQUET FORMATTED DATA -----
CREATE EXTERNAL TABLE STAGING_AIRLINE
    USING parquet OPTIONS(path 's3a://AKIAILHSQ3FINHV473RQ:Tnn6GOHhjaayIRtVNApYQgNLU3FxXkw9albr9hVJ@zeppelindemo/airlinedata_cleaned');
    -- USING parquet OPTIONS(path '/home/swati/snappy-commons/examples/quickstart/data/airlineParquetData');

----- CREATE COLUMN TABLE -----
CREATE TABLE AIRLINE USING column OPTIONS(PERSISTENT 'SYNCHRONOUS', BUCKETS '128', EVICTION_BY 'LRUHEAPPERCENT', overflow 'true')  AS (
  SELECT monotonically_increasing_id() as id, '' as dummy,  Year AS Year_, Month AS Month_ , DayOfMonth, DepTime, CRSDepTime, ArrTime, UniqueCarrier, ArrDelay, DepDelay, Origin,
    Dest, Distance, TaxiIn, TaxiOut, Cancelled, CarrierDelay, WeatherDelay
    FROM STAGING_AIRLINE);

-- DROP TABLE IF ALREADY EXISTS --
DROP TABLE IF EXISTS STAGING_AIRLINEREF;
DROP TABLE IF EXISTS AIRLINEREF;

----- CREATE TEMPORARY STAGING TABLE TO LOAD PARQUET FORMATTED DATA -----
CREATE EXTERNAL TABLE STAGING_AIRLINEREF
    USING parquet OPTIONS(path 's3a://AKIAILHSQ3FINHV473RQ:Tnn6GOHhjaayIRtVNApYQgNLU3FxXkw9albr9hVJ@zeppelindemo/airportdata');
    --USING parquet OPTIONS(path '/home/swati/snappy-commons/examples/quickstart/data/airportcodeParquetData');

----- CREATE ROW TABLE -----

CREATE TABLE AIRLINEREF USING row OPTIONS(PERSISTENT 'SYNCHRONOUS') AS (SELECT CODE, DESCRIPTION FROM STAGING_AIRLINEREF);