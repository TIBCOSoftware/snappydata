DROP TABLE IF EXISTS ADJUSTMENT;
DROP TABLE IF EXISTS staging_ADJUSTMENT;
----- CREATE TEMPORARY STAGING TABLE TO LOAD CSV FORMATTED DATA -----
CREATE EXTERNAL TABLE staging_ADJUSTMENT
    USING com.databricks.spark.csv OPTIONS (path '/export/shared/QA_DATA/TMG_Data_20G/ADJUSTMENT.dat', header 'true', inferSchema 'true', nullValue 'NULL', maxCharsPerColumn '4096');
CREATE TABLE ADJUSTMENT USING column OPTIONS(partition_by 'BILL_ENT_ID',buckets '32',key_columns 'CLIENT_ID,BILL_ENT_ID,ADJ_ID ' ) AS (SELECT * FROM staging_ADJUSTMENT);

DROP TABLE IF EXISTS AGREEMENT;
DROP TABLE IF EXISTS staging_AGREEMENT;
  ----- CREATE TEMPORARY STAGING TABLE TO LOAD CSV FORMATTED DATA -----
CREATE EXTERNAL TABLE staging_AGREEMENT
    USING com.databricks.spark.csv OPTIONS (path '/export/shared/QA_DATA/TMG_Data_20G/AGREEMENT.dat', header 'true', inferSchema 'true', nullValue 'NULL', maxCharsPerColumn '4096');
CREATE TABLE AGREEMENT USING column OPTIONS(partition_by 'AGREE_ID', buckets '32',key_columns 'CLIENT_ID,AGREE_ID ' ) AS (SELECT * FROM staging_AGREEMENT);

