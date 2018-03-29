DROP TABLE IF EXISTS ADJUSTMENT;
DROP TABLE IF EXISTS staging_ADJUSTMENT;
----- CREATE TEMPORARY STAGING TABLE TO LOAD CSV FORMATTED DATA -----
CREATE EXTERNAL TABLE staging_ADJUSTMENT
    USING com.databricks.spark.csv OPTIONS (path ':dataLocation/ADJUSTMENT.dat', header 'true', inferSchema 'true', nullValue 'NULL', maxCharsPerColumn '4096');
CREATE TABLE ADJUSTMENT USING column OPTIONS(partition_by 'BILL_ENT_ID',buckets '32',key_columns 'CLIENT_ID,BILL_ENT_ID,ADJ_ID ' ) AS (SELECT * FROM staging_ADJUSTMENT);

DROP TABLE IF EXISTS AGREEMENT;
DROP TABLE IF EXISTS staging_AGREEMENT;
  ----- CREATE TEMPORARY STAGING TABLE TO LOAD CSV FORMATTED DATA -----
CREATE EXTERNAL TABLE staging_AGREEMENT
    USING com.databricks.spark.csv OPTIONS (path ':dataLocation/AGREEMENT.dat', header 'true', inferSchema 'true', nullValue 'NULL', maxCharsPerColumn '4096');
CREATE TABLE AGREEMENT USING column OPTIONS(partition_by 'AGREE_ID', buckets '32',key_columns 'CLIENT_ID,AGREE_ID ' ) AS (SELECT * FROM staging_AGREEMENT);

DROP TABLE IF EXISTS BENEFIT_GROUPS;
DROP TABLE IF EXISTS staging_BENEFIT_GROUPS;
----- CREATE TEMPORARY STAGING TABLE TO LOAD CSV FORMATTED DATA -----
CREATE EXTERNAL TABLE staging_BENEFIT_GROUPS
    USING com.databricks.spark.csv OPTIONS (path ':dataLocation/BENEFIT_GROUPS.dat', header 'true', inferSchema 'true', nullValue 'NULL', maxCharsPerColumn '4096');
CREATE TABLE BENEFIT_GROUPS USING column OPTIONS(partition_by 'GRP_ID', buckets '32',key_columns 'CLIENT_ID,GRP_ID,BENE_PKG_ID,BENE_GRP_ID ' ) AS (SELECT * FROM staging_BENEFIT_GROUPS);

DROP TABLE IF EXISTS BENEFIT_PACKAGE;
DROP TABLE IF EXISTS staging_BENEFIT_PACKAGE;
----- CREATE TEMPORARY STAGING TABLE TO LOAD CSV FORMATTED DATA -----
CREATE EXTERNAL TABLE staging_BENEFIT_PACKAGE
    USING com.databricks.spark.csv OPTIONS (path ':dataLocation/BENEFIT_PACKAGE.dat', header 'true', inferSchema 'true', nullValue 'NULL', maxCharsPerColumn '4096');
CREATE TABLE BENEFIT_PACKAGE USING column OPTIONS(partition_by 'BENE_PKG_ID', buckets '32',key_columns 'CLIENT_ID,BENE_PKG_ID' ) AS (SELECT * FROM staging_BENEFIT_PACKAGE);