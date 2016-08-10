-- DROP TABLE IF ALREADY EXISTS --
DROP TABLE IF EXISTS STAGING_AIRLINEREF;
DROP TABLE IF EXISTS AIRLINEREF;

----- CREATE TEMPORARY STAGING TABLE TO LOAD PARQUET FORMATTED DATA -----
CREATE EXTERNAL TABLE STAGING_AIRLINEREF
    USING parquet OPTIONS(path ':path');

----- CREATE ROW TABLE -----

CREATE TABLE AIRLINEREF USING row OPTIONS(PERSISTENT "async") AS (SELECT CODE, DESCRIPTION FROM STAGING_AIRLINEREF);
