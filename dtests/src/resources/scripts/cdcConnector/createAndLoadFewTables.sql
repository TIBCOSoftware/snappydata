DROP TABLE IF EXISTS ADJUSTMENT;
DROP TABLE IF EXISTS staging_ADJUSTMENT;
----- CREATE TEMPORARY STAGING TABLE TO LOAD CSV FORMATTED DATA -----
CREATE EXTERNAL TABLE staging_ADJUSTMENT
    USING com.databricks.spark.csv OPTIONS (path ':dataLocation/ADJUSTMENT.dat', header 'true', inferSchema 'true', nullValue 'NULL', maxCharsPerColumn '4096');
CREATE TABLE ADJUSTMENT USING column OPTIONS(partition_by 'BILL_ENT_ID',buckets '32',key_columns 'CLIENT_ID,BILL_ENT_ID,ADJ_ID ' ) AS (SELECT * FROM staging_ADJUSTMENT);