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

DROP TABLE IF EXISTS BANK;
DROP TABLE IF EXISTS staging_BANK;
----- CREATE TEMPORARY STAGING TABLE TO LOAD CSV FORMATTED DATA -----
CREATE EXTERNAL TABLE staging_BANK
    USING com.databricks.spark.csv OPTIONS (path '/export/shared/QA_DATA/TMG_Data_20G/BANK.dat', header 'true', inferSchema 'true', nullValue 'NULL', maxCharsPerColumn '4096');
CREATE TABLE BANK USING column OPTIONS(partition_by 'BNK_ORG_ID', buckets '32',key_columns 'CLIENT_ID,BNK_ORG_ID,BNK_ID ' ) AS (SELECT * FROM staging_BANK);

DROP TABLE IF EXISTS BANK_ACCOUNT;
DROP TABLE IF EXISTS staging_BANK_ACCOUNT;
----- CREATE TEMPORARY STAGING TABLE TO LOAD CSV FORMATTED DATA -----
CREATE EXTERNAL TABLE staging_BANK_ACCOUNT
    USING com.databricks.spark.csv OPTIONS (path '/export/shared/QA_DATA/TMG_Data_20G/BANK_ACCOUNT.dat', header 'true', inferSchema 'true', nullValue 'NULL', maxCharsPerColumn '4096');
CREATE TABLE BANK_ACCOUNT USING column OPTIONS(partition_by 'BNK_ORG_ID',buckets '32',key_columns 'CLIENT_ID,BNK_ORG_ID,BNK_ID,BNK_ACCT_ID ' ) AS (SELECT * FROM staging_BANK_ACCOUNT);

DROP TABLE IF EXISTS BENEFIT_GROUP_NAME;
DROP TABLE IF EXISTS staging_BENEFIT_GROUP_NAME;
----- CREATE TEMPORARY STAGING TABLE TO LOAD CSV FORMATTED DATA -----
CREATE EXTERNAL TABLE staging_BENEFIT_GROUP_NAME
    USING com.databricks.spark.csv OPTIONS (path '/export/shared/QA_DATA/TMG_Data_20G/BENEFIT_GROUP_NAME.dat', header 'true', inferSchema 'true', nullValue 'NULL', maxCharsPerColumn '4096');
CREATE TABLE BENEFIT_GROUP_NAME USING column OPTIONS(partition_by 'GRP_ID', buckets '32',key_columns 'CLIENT_ID,GRP_ID,BENE_GRP_ID,BENE_GRP_NM_ID ' ) AS (SELECT * FROM staging_BENEFIT_GROUP_NAME);

DROP TABLE IF EXISTS BENEFIT_GROUPS;
DROP TABLE IF EXISTS staging_BENEFIT_GROUPS;
----- CREATE TEMPORARY STAGING TABLE TO LOAD CSV FORMATTED DATA -----
CREATE EXTERNAL TABLE staging_BENEFIT_GROUPS
    USING com.databricks.spark.csv OPTIONS (path '/export/shared/QA_DATA/TMG_Data_20G/BENEFIT_GROUPS.dat', header 'true', inferSchema 'true', nullValue 'NULL', maxCharsPerColumn '4096');
CREATE TABLE BENEFIT_GROUPS USING column OPTIONS(partition_by 'GRP_ID', buckets '32',key_columns 'CLIENT_ID,GRP_ID,BENE_PKG_ID,BENE_GRP_ID ' ) AS (SELECT * FROM staging_BENEFIT_GROUPS);

DROP TABLE IF EXISTS BENEFIT_PACKAGE;
DROP TABLE IF EXISTS staging_BENEFIT_PACKAGE;
----- CREATE TEMPORARY STAGING TABLE TO LOAD CSV FORMATTED DATA -----
CREATE EXTERNAL TABLE staging_BENEFIT_PACKAGE
    USING com.databricks.spark.csv OPTIONS (path '/export/shared/QA_DATA/TMG_Data_20G/BENEFIT_PACKAGE.dat', header 'true', inferSchema 'true', nullValue 'NULL', maxCharsPerColumn '4096');
CREATE TABLE BENEFIT_PACKAGE USING column OPTIONS(partition_by 'BENE_PKG_ID', buckets '32',key_columns 'CLIENT_ID,BENE_PKG_ID' ) AS (SELECT * FROM staging_BENEFIT_PACKAGE);
