----- CREATE TEMP TABLE -----
CREATE TABLE IF NOT EXISTS AIRLINEREF_TEMP
 (
   CODE VARCHAR(25),
   DESCRIPTION VARCHAR(25)
 ) 
 USING parquet OPTIONS(path '/home/supriya/snappy/snappy-commons/snappy-examples/quickstart/data/airportcodeParquetData');

----- CREATE ROW TABLE -----

CREATE TABLE IF NOT EXISTS AIRLINEREF USING row OPTIONS() AS (SELECT * FROM AIRLINEREF_TEMP);
