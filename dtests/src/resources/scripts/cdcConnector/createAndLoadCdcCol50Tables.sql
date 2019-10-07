CREATE EXTERNAL TABLE staging_PERSON_EVENT
    USING com.databricks.spark.csv OPTIONS (path '/export/shared/QA_DATA/USER2_Data/DataGenerator/PERSON_EVENT.dat', header 'true', inferSchema 'true', nullValue 'NULL', maxCharsPerColumn '4096');
CREATE TABLE PERSON_EVENT USING column OPTIONS(partition_by 'PRSN_EVNT_ID', buckets '32',key_columns 'CLIENT_ID,PRSN_EVNT_ID ') AS (SELECT * FROM staging_PERSON_EVENT);

CREATE EXTERNAL TABLE staging_PERSON_EVENT_ATTRIBUTE
    USING com.databricks.spark.csv OPTIONS (path '/export/shared/QA_DATA/USER2_Data/DataGenerator/PERSON_EVENT_ATTRIBUTE.dat', header 'true', inferSchema 'true', nullValue 'NULL', maxCharsPerColumn '4096');
CREATE TABLE PERSON_EVENT_ATTRIBUTE USING column OPTIONS(partition_by 'PRSN_EVNT_ID', buckets '32',key_columns 'CLIENT_ID,PRSN_EVNT_ID,PRSN_EVNT_ATTR_ID ') AS (SELECT * FROM staging_PERSON_EVENT_ATTRIBUTE);

CREATE EXTERNAL TABLE staging_CLAIM_STATUS
    USING com.databricks.spark.csv OPTIONS (path '/export/shared/QA_DATA/USER2_Data/DataGenerator/CLAIM_STATUS.dat', header 'true', inferSchema 'true', nullValue 'NULL', maxCharsPerColumn '4096');
CREATE TABLE CLAIM_STATUS USING column OPTIONS(partition_by 'PRSN_ID', buckets '32',key_columns 'CLIENT_ID,PRSN_ID,CLM_ID,SEQ_NUM,CLM_STAT_ID ') AS (SELECT * FROM staging_CLAIM_STATUS);

CREATE EXTERNAL TABLE staging_CLAIM_ADDITIONAL_DIAGNOSIS
    USING com.databricks.spark.csv OPTIONS (path '/export/shared/QA_DATA/USER2_Data/DataGenerator/CLAIM_ADDITIONAL_DIAGNOSIS.dat', header 'true', inferSchema 'true', nullValue 'NULL', maxCharsPerColumn '4096');
CREATE TABLE CLAIM_ADDITIONAL_DIAGNOSIS USING column OPTIONS(partition_by 'PRSN_ID', buckets '32',key_columns 'CLIENT_ID,PRSN_ID,CLM_ID,CLM_ADD_DIAG_ID ') AS (SELECT * FROM staging_CLAIM_ADDITIONAL_DIAGNOSIS);

CREATE EXTERNAL TABLE staging_CLAIM_DETAIL
    USING com.databricks.spark.csv OPTIONS (path '/export/shared/QA_DATA/USER2_Data/DataGenerator/CLAIM_DETAIL.dat', header 'true', inferSchema 'true', nullValue 'NULL', maxCharsPerColumn '4096');
CREATE TABLE CLAIM_DETAIL USING column OPTIONS(partition_by 'PRSN_ID', buckets '32',key_columns 'CLIENT_ID,PRSN_ID,CLM_ID,SEQ_NUM,CLM_DTL_ID ') AS (SELECT * FROM staging_CLAIM_DETAIL);

CREATE EXTERNAL TABLE staging_CLAIM_PAYMENT_DETAIL
    USING com.databricks.spark.csv OPTIONS (path '/export/shared/QA_DATA/USER2_Data/DataGenerator/CLAIM_PAYMENT_DETAIL.dat', header 'true', inferSchema 'true', nullValue 'NULL', maxCharsPerColumn '4096');
CREATE TABLE CLAIM_PAYMENT_DETAIL USING column OPTIONS(partition_by 'PRSN_ID',buckets '32',key_columns 'CLIENT_ID,PRSN_ID,CLM_ID,CLM_PAY_ID,CLM_PAY_DTL_ID ') AS (SELECT * FROM staging_CLAIM_PAYMENT_DETAIL);

  CREATE EXTERNAL TABLE staging_CLAIM_ATTRIBUTE
      USING com.databricks.spark.csv OPTIONS (path '/export/shared/QA_DATA/USER2_Data/DataGenerator/CLAIM_ATTRIBUTE.dat', header 'true', inferSchema 'true', nullValue 'NULL', maxCharsPerColumn '4096');
  CREATE TABLE CLAIM_ATTRIBUTE USING column OPTIONS(partition_by 'PRSN_ID', buckets '32',key_columns 'CLIENT_ID,PRSN_ID,CLM_ID,CLM_ATTR_ID ') AS (SELECT * FROM staging_CLAIM_ATTRIBUTE);

CREATE EXTERNAL TABLE staging_CLAIM
  USING com.databricks.spark.csv OPTIONS (path '/export/shared/QA_DATA/USER2_Data/DataGenerator/CLAIM.dat', header 'true', inferSchema 'true', nullValue 'NULL', maxCharsPerColumn '4096');
CREATE TABLE CLAIM USING column OPTIONS(partition_by 'PRSN_ID', buckets '32',key_columns 'CLIENT_ID,PRSN_ID,CLM_ID ') AS (SELECT * FROM staging_CLAIM);

CREATE EXTERNAL TABLE staging_PERSON_CONTACT
    USING com.databricks.spark.csv OPTIONS (path '/export/shared/QA_DATA/USER2_Data/DataGenerator/PERSON_CONTACT.dat', header 'true', inferSchema 'true', nullValue 'NULL', maxCharsPerColumn '4096');
CREATE TABLE PERSON_CONTACT USING column OPTIONS(partition_by 'PRSN_ID',buckets '32',key_columns 'CLIENT_ID,PRSN_ID,CNTC_ID,PRSN_CNTC_ID ') AS (SELECT * FROM staging_PERSON_CONTACT);

CREATE EXTERNAL TABLE staging_ORGANIZATION_CODE
    USING com.databricks.spark.csv OPTIONS (path '/export/shared/QA_DATA/USER2_Data/DataGenerator/ORGANIZATION_CODE.dat', header 'true', inferSchema 'true', nullValue 'NULL', maxCharsPerColumn '4096');
CREATE TABLE ORGANIZATION_CODE USING column OPTIONS(partition_by 'ORG_ID', buckets '32',key_columns 'CLIENT_ID,ORG_ID,CD_VAL_ID,ORG_CD_ID ') AS (SELECT * FROM staging_ORGANIZATION_CODE);

CREATE EXTERNAL TABLE staging_COMPLAINT_STATUS
    USING com.databricks.spark.csv OPTIONS (path '/export/shared/QA_DATA/USER2_Data/DataGenerator/COMPLAINT_STATUS.dat', header 'true', inferSchema 'true', nullValue 'NULL', maxCharsPerColumn '4096');
CREATE TABLE COMPLAINT_STATUS USING column OPTIONS(partition_by 'INQ_ID',buckets '32',key_columns 'CLIENT_ID,INQ_ID,COMPLAINT_ID,COMPLAINT_STAT_ID ' ) AS (SELECT * FROM staging_COMPLAINT_STATUS);

CREATE EXTERNAL TABLE staging_CONTACT
    USING com.databricks.spark.csv OPTIONS (path '/export/shared/QA_DATA/USER2_Data/DataGenerator/CONTACT.dat', header 'true', inferSchema 'true', nullValue 'NULL', maxCharsPerColumn '4096');
CREATE TABLE CONTACT USING column OPTIONS(partition_by 'CNTC_ID', buckets '32',key_columns 'CLIENT_ID,CNTC_ID' ) AS (SELECT * FROM staging_CONTACT);

CREATE EXTERNAL TABLE staging_CLAIM_PAYMENT
    USING com.databricks.spark.csv OPTIONS (path '/export/shared/QA_DATA/USER2_Data/DataGenerator/CLAIM_PAYMENT.dat', header 'true', inferSchema 'true', nullValue 'NULL', maxCharsPerColumn '4096');
CREATE TABLE CLAIM_PAYMENT USING column OPTIONS(partition_by 'CLM_PAY_ID', buckets '32',key_columns 'CLIENT_ID,CLM_PAY_ID ' ) AS (SELECT * FROM staging_CLAIM_PAYMENT);

CREATE EXTERNAL TABLE staging_TOPIC_COMMUNICATION
    USING com.databricks.spark.csv OPTIONS (path '/export/shared/QA_DATA/USER2_Data/DataGenerator/TOPIC_COMMUNICATION.dat', header 'true', inferSchema 'true', nullValue 'NULL', maxCharsPerColumn '4096');
CREATE TABLE TOPIC_COMMUNICATION USING column OPTIONS(partition_by 'CMCN_INQ_ID', buckets '32',key_columns ' CLIENT_ID,CMCN_INQ_ID,TPC_INQ_ID,CMCN_ID,TPC_ID' ) AS (SELECT * FROM staging_TOPIC_COMMUNICATION);

CREATE EXTERNAL TABLE staging_CONTACT_TYPE_CONTACT
USING com.databricks.spark.csv OPTIONS (path '/export/shared/QA_DATA/USER2_Data/DataGenerator/CONTACT_TYPE_CONTACT.dat', header 'true', inferSchema 'true', nullValue 'NULL', maxCharsPerColumn '4096');
CREATE TABLE CONTACT_TYPE_CONTACT USING column OPTIONS(partition_by 'CNTC_ID', buckets '32',key_columns 'CLIENT_ID,CNTC_ID,ORG_CNTC_TYP_ID,CNTC_TYP_CNTC_ID ' ) AS (SELECT * FROM staging_CONTACT_TYPE_CONTACT);

CREATE EXTERNAL TABLE staging_TOPIC
    USING com.databricks.spark.csv OPTIONS (path '/export/shared/QA_DATA/USER2_Data/DataGenerator/TOPIC.dat', header 'true', inferSchema 'true', nullValue 'NULL', maxCharsPerColumn '4096');
CREATE TABLE TOPIC USING column OPTIONS(partition_by 'INQ_ID',buckets '32',key_columns 'CLIENT_ID,INQ_ID,TPC_ID ' ) AS (SELECT * FROM staging_TOPIC);

----- CREATE TEMPORARY STAGING TABLE TO LOAD CSV FORMATTED DATA -----
CREATE EXTERNAL TABLE staging_LINE_ADDITIONAL_DIAGNOSIS
    USING com.databricks.spark.csv OPTIONS (path '/export/shared/QA_DATA/USER2_Data/DataGenerator/LINE_ADDITIONAL_DIAGNOSIS.dat', header 'true', inferSchema 'true', nullValue 'NULL', maxCharsPerColumn '4096');
CREATE TABLE LINE_ADDITIONAL_DIAGNOSIS USING column OPTIONS(partition_by 'PRSN_ID',buckets '32',key_columns 'CLIENT_ID,PRSN_ID,CLM_ID,SEQ_NUM,CLM_ADD_DIAG_ID,LN_ADD_DIAG_ID ' ) AS (SELECT * FROM staging_LINE_ADDITIONAL_DIAGNOSIS);


CREATE EXTERNAL TABLE staging_PROCEDURE_CODE
    USING com.databricks.spark.csv OPTIONS (path '/export/shared/QA_DATA/USER2_Data/DataGenerator/PROCEDURE_CODE.dat', header 'true', inferSchema 'true', nullValue 'NULL', maxCharsPerColumn '4096');
    CREATE TABLE PROCEDURE_CODE USING column OPTIONS(partition_by 'PR_CD_ID', buckets '32',key_columns 'CLIENT_ID,PR_CD_ID ' ) AS (SELECT * FROM staging_PROCEDURE_CODE);

CREATE EXTERNAL TABLE staging_CODE_VALUE
    USING com.databricks.spark.csv OPTIONS (path '/export/shared/QA_DATA/USER2_Data/DataGenerator/CODE_VALUE.dat', header 'true', inferSchema 'true', nullValue 'NULL', maxCharsPerColumn '4096');
CREATE TABLE CODE_VALUE USING column OPTIONS(partition_by 'CD_VAL_ID', buckets '32',key_columns 'CLIENT_ID,CD_VAL_ID ' ) AS (SELECT * FROM staging_CODE_VALUE);

CREATE EXTERNAL TABLE staging_POSTAL_ADDRESS
            USING com.databricks.spark.csv OPTIONS (path '/export/shared/QA_DATA/USER2_Data/DataGenerator/POSTAL_ADDRESS.dat', header 'true', inferSchema 'true', nullValue 'NULL', maxCharsPerColumn '4096');
CREATE TABLE POSTAL_ADDRESS USING column OPTIONS(partition_by 'CNTC_ID',key_columns 'CLIENT_ID,CNTC_ID,PSTL_ADDR_ID') AS (SELECT * FROM staging_POSTAL_ADDRESS);

----- CREATE TEMPORARY STAGING TABLE TO LOAD CSV FORMATTED DATA -----
CREATE EXTERNAL TABLE staging_ADJUSTMENT
    USING com.databricks.spark.csv OPTIONS (path '/export/shared/QA_DATA/USER2_Data_20G/ADJUSTMENT.dat', header 'true', inferSchema 'true', nullValue 'NULL', maxCharsPerColumn '4096');
CREATE TABLE ADJUSTMENT USING column OPTIONS(partition_by 'BILL_ENT_ID',buckets '32',key_columns 'CLIENT_ID,BILL_ENT_ID,ADJ_ID ' ) AS (SELECT * FROM staging_ADJUSTMENT);


  CREATE EXTERNAL TABLE staging_AGREEMENT
      USING com.databricks.spark.csv OPTIONS (path '/export/shared/QA_DATA/USER2_Data_20G/AGREEMENT.dat', header 'true', inferSchema 'true', nullValue 'NULL', maxCharsPerColumn '4096');
  CREATE TABLE AGREEMENT USING column OPTIONS(partition_by 'AGREE_ID', buckets '32',key_columns 'CLIENT_ID,AGREE_ID ' ) AS (SELECT * FROM staging_AGREEMENT);


  CREATE EXTERNAL TABLE staging_BANK_ACCOUNT
      USING com.databricks.spark.csv OPTIONS (path '/export/shared/QA_DATA/USER2_Data_20G/BANK_ACCOUNT.dat', header 'true', inferSchema 'true', nullValue 'NULL', maxCharsPerColumn '4096');
  CREATE TABLE BANK_ACCOUNT USING column OPTIONS(partition_by 'BNK_ORG_ID',buckets '32',key_columns 'CLIENT_ID,BNK_ORG_ID,BNK_ID,BNK_ACCT_ID ' ) AS (SELECT * FROM staging_BANK_ACCOUNT);


CREATE EXTERNAL TABLE staging_BANK
    USING com.databricks.spark.csv OPTIONS (path '/export/shared/QA_DATA/USER2_Data_20G/BANK.dat', header 'true', inferSchema 'true', nullValue 'NULL', maxCharsPerColumn '4096');
CREATE TABLE BANK USING column OPTIONS(partition_by 'BNK_ORG_ID', buckets '32',key_columns 'CLIENT_ID,BNK_ORG_ID,BNK_ID ' ) AS (SELECT * FROM staging_BANK);

CREATE EXTERNAL TABLE staging_BENEFIT_GROUP_NAME
    USING com.databricks.spark.csv OPTIONS (path '/export/shared/QA_DATA/USER2_Data_20G/BENEFIT_GROUP_NAME.dat', header 'true', inferSchema 'true', nullValue 'NULL', maxCharsPerColumn '4096');
CREATE TABLE BENEFIT_GROUP_NAME USING column OPTIONS(partition_by 'GRP_ID', buckets '32',key_columns 'CLIENT_ID,GRP_ID,BENE_GRP_ID,BENE_GRP_NM_ID ' ) AS (SELECT * FROM staging_BENEFIT_GROUP_NAME);

CREATE EXTERNAL TABLE staging_BENEFIT_GROUPS
    USING com.databricks.spark.csv OPTIONS (path '/export/shared/QA_DATA/USER2_Data_20G/BENEFIT_GROUPS.dat', header 'true', inferSchema 'true', nullValue 'NULL', maxCharsPerColumn '4096');
CREATE TABLE BENEFIT_GROUPS USING column OPTIONS(partition_by 'GRP_ID', buckets '32',key_columns 'CLIENT_ID,GRP_ID,BENE_PKG_ID,BENE_GRP_ID ' ) AS (SELECT * FROM staging_BENEFIT_GROUPS);

  CREATE EXTERNAL TABLE staging_BENEFIT_PACKAGE_ATTRIBUTE
      USING com.databricks.spark.csv OPTIONS (path '/export/shared/QA_DATA/USER2_Data_20G/BENEFIT_PACKAGE_ATTRIBUTE.dat', header 'true', inferSchema 'true', nullValue 'NULL', maxCharsPerColumn '4096');
  CREATE TABLE BENEFIT_PACKAGE_ATTRIBUTE USING column OPTIONS(partition_by 'BENE_PKG_ID', buckets '32',key_columns 'CLIENT_ID,BENE_PKG_ID,BENE_PKG_ATTR_ID ' ) AS (SELECT * FROM staging_BENEFIT_PACKAGE_ATTRIBUTE);

  CREATE EXTERNAL TABLE staging_BENEFIT_PACKAGE
      USING com.databricks.spark.csv OPTIONS (path '/export/shared/QA_DATA/USER2_Data_20G/BENEFIT_PACKAGE.dat', header 'true', inferSchema 'true', nullValue 'NULL', maxCharsPerColumn '4096');
  CREATE TABLE BENEFIT_PACKAGE USING column OPTIONS(partition_by 'BENE_PKG_ID', buckets '32',key_columns 'CLIENT_ID,BENE_PKG_ID' ) AS (SELECT * FROM staging_BENEFIT_PACKAGE);

  CREATE EXTERNAL TABLE staging_BENEFIT_PACKAGE_RELATION
      USING com.databricks.spark.csv OPTIONS (path '/export/shared/QA_DATA/USER2_Data_20G/BENEFIT_PACKAGE_RELATION.dat', header 'true', inferSchema 'true', nullValue 'NULL', maxCharsPerColumn '4096');
  CREATE TABLE BENEFIT_PACKAGE_RELATION USING column OPTIONS(partition_by 'BENE_PKG_ID', buckets '32',key_columns 'CLIENT_ID,BENE_PKG_ID,PKG_RELN_ID ' ) AS (SELECT * FROM staging_BENEFIT_PACKAGE_RELATION);

  CREATE EXTERNAL TABLE staging_BILLING_ENTITY_CONTACT
      USING com.databricks.spark.csv OPTIONS (path '/export/shared/QA_DATA/USER2_Data_20G/BILLING_ENTITY_CONTACT.dat', header 'true', inferSchema 'true', nullValue 'NULL', maxCharsPerColumn '4096');
  CREATE TABLE BILLING_ENTITY_CONTACT USING column OPTIONS(partition_by 'BILL_ENT_ID',buckets '32',key_columns 'CLIENT_ID,BILL_ENT_ID,CNTC_ID,BILL_ENT_CNTC_ID ' ) AS (SELECT * FROM staging_BILLING_ENTITY_CONTACT);

  CREATE EXTERNAL TABLE staging_BILLING_ENTITY
      USING com.databricks.spark.csv OPTIONS (path '/export/shared/QA_DATA/USER2_Data_20G/BILLING_ENTITY.dat', header 'true', inferSchema 'true', nullValue 'NULL', maxCharsPerColumn '4096');
  CREATE TABLE BILLING_ENTITY USING column OPTIONS(partition_by 'BILL_ENT_ID', buckets '32',key_columns 'CLIENT_ID,BILL_ENT_ID') AS (SELECT * FROM staging_BILLING_ENTITY);

  CREATE EXTERNAL TABLE staging_BILLING_ENTITY_DETAIL
      USING com.databricks.spark.csv OPTIONS (path '/export/shared/QA_DATA/USER2_Data_20G/BILLING_ENTITY_DETAIL.dat', header 'true', inferSchema 'true', nullValue 'NULL', maxCharsPerColumn '4096');
  CREATE TABLE BILLING_ENTITY_DETAIL USING column OPTIONS(partition_by 'BILL_ENT_ID', buckets '32',key_columns 'CLIENT_ID,BILL_ENT_ID ' ) AS (SELECT * FROM staging_BILLING_ENTITY_DETAIL);

  CREATE EXTERNAL TABLE staging_BILLING_ENTITY_SCHEDULE
     USING com.databricks.spark.csv OPTIONS (path '/export/shared/QA_DATA/USER2_Data_20G/BILLING_ENTITY_SCHEDULE.dat', header 'true', inferSchema 'true', nullValue 'NULL', maxCharsPerColumn '4096');
  CREATE TABLE BILLING_ENTITY_SCHEDULE USING column OPTIONS(partition_by 'BILL_ENT_ID', buckets '32',key_columns 'CLIENT_ID,BILL_ENT_ID,BILL_SCHD_ID,BILL_ENT_SCHD_ID ' ) AS (SELECT * FROM staging_BILLING_ENTITY_SCHEDULE);

    CREATE EXTERNAL TABLE staging_BILLING_RECONCILIATION
        USING com.databricks.spark.csv OPTIONS (path '/export/shared/QA_DATA/USER2_Data_20G/BILLING_RECONCILIATION.dat', header 'true', inferSchema 'true', nullValue 'NULL', maxCharsPerColumn '4096');
    CREATE TABLE BILLING_RECONCILIATION USING column OPTIONS(partition_by 'BILL_ENT_ID', buckets '32',key_columns 'CLIENT_ID,BILL_ENT_ID,BILL_RECON_ID ' ) AS (SELECT * FROM staging_BILLING_RECONCILIATION);

  CREATE EXTERNAL TABLE staging_BILLING_SCHEDULE
      USING com.databricks.spark.csv OPTIONS (path '/export/shared/QA_DATA/USER2_Data_20G/BILLING_SCHEDULE.dat', header 'true', inferSchema 'true', nullValue 'NULL', maxCharsPerColumn '4096');
  CREATE TABLE BILLING_SCHEDULE USING column OPTIONS(partition_by 'BILL_SCHD_ID', buckets '32',key_columns 'CLIENT_ID,BILL_SCHD_ID ' ) AS (SELECT * FROM staging_BILLING_SCHEDULE);

    CREATE EXTERNAL TABLE staging_BILLING_SOURCE
        USING com.databricks.spark.csv OPTIONS (path '/export/shared/QA_DATA/USER2_Data_20G/BILLING_SOURCE.dat', header 'true', inferSchema 'true', nullValue 'NULL', maxCharsPerColumn '4096');
    CREATE TABLE BILLING_SOURCE USING column OPTIONS(partition_by 'BILL_ENT_ID', buckets '32',key_columns 'CLIENT_ID,BILL_ENT_ID,SRC_TYP_REF_ID,BILL_SRC_ID ' ) AS (SELECT * FROM staging_BILLING_SOURCE);

    CREATE EXTERNAL TABLE staging_CHARGE_ITEM
        USING com.databricks.spark.csv OPTIONS (path '/export/shared/QA_DATA/USER2_Data_20G/CHARGE_ITEM.dat', header 'true', inferSchema 'true', nullValue 'NULL', maxCharsPerColumn '4096');
    CREATE TABLE CHARGE_ITEM USING column OPTIONS(partition_by 'BILL_ENT_ID', buckets '32',key_columns 'CLIENT_ID,BILL_ENT_ID,BILL_ENT_SCHD_ID,CHRG_ITM_ID ' ) AS (SELECT * FROM staging_CHARGE_ITEME);

  CREATE EXTERNAL TABLE staging_CHECKS
      USING com.databricks.spark.csv OPTIONS (path '/export/shared/QA_DATA/USER2_Data_20G/CHECKS.dat', header 'true', inferSchema 'true', nullValue 'NULL', maxCharsPerColumn '4096');
  CREATE TABLE CHECKS USING column OPTIONS(partition_by 'CLM_PAY_ID', buckets '32',key_columns 'CLIENT_ID,CLM_PAY_ID,CHK_ID ' ) AS (SELECT * FROM staging_CHECKS);

  CREATE EXTERNAL TABLE staging_CHECK_STATUS
      USING com.databricks.spark.csv OPTIONS (path '/export/shared/QA_DATA/USER2_Data_20G/CHECK_STATUS.dat', header 'true', inferSchema 'true', nullValue 'NULL', maxCharsPerColumn '4096');
  CREATE TABLE CHECK_STATUS USING column OPTIONS(partition_by 'CLM_PAY_ID',buckets '32',key_columns 'CLIENT_ID,CLM_PAY_ID,CHK_ID,CHK_STAT_ID ' ) AS (SELECT * FROM staging_CHECK_STATUS);

  CREATE EXTERNAL TABLE staging_CLAIM_COB
      USING com.databricks.spark.csv OPTIONS (path '/export/shared/QA_DATA/USER2_Data_20G/CLAIM_COB.dat', header 'true', inferSchema 'true', nullValue 'NULL', maxCharsPerColumn '4096');
  CREATE TABLE CLAIM_COB USING column OPTIONS(partition_by 'PRSN_ID',buckets '32',key_columns 'CLIENT_ID,PRSN_ID,CLM_ID,CLM_COB_ID ' ) AS (SELECT * FROM staging_CLAIM_COB);

  CREATE EXTERNAL TABLE staging_CLAIM_COSHARE_TRACKING
      USING com.databricks.spark.csv OPTIONS (path '/export/shared/QA_DATA/USER2_Data_20G/CLAIM_COSHARE_TRACKING.dat', header 'true', inferSchema 'true', nullValue 'NULL', maxCharsPerColumn '4096');
  CREATE TABLE CLAIM_COSHARE_TRACKING USING column OPTIONS(partition_by 'PRSN_ID',buckets '32',key_columns 'CLIENT_ID,PRSN_ID,CLAIM_ID,LINE_NO ' ) AS (SELECT * FROM staging_CLAIM_COSHARE_TRACKING);

 CREATE EXTERNAL TABLE staging_CLAIM_HOSPITAL
      USING com.databricks.spark.csv OPTIONS (path '/export/shared/QA_DATA/USER2_Data_20G/CLAIM_HOSPITAL.dat', header 'true', inferSchema 'true', nullValue 'NULL', maxCharsPerColumn '4096');
  CREATE TABLE CLAIM_HOSPITAL USING column OPTIONS(partition_by 'PRSN_ID',buckets '32',key_columns 'CLIENT_ID,PRSN_ID,CLM_ID,CLM_HOSP_ID ' ) AS (SELECT * FROM staging_CLAIM_HOSPITAL);

  CREATE EXTERNAL TABLE staging_CLAIM_LINE_ATTRIBUTE
      USING com.databricks.spark.csv OPTIONS (path '/export/shared/QA_DATA/USER2_Data_20G/CLAIM_LINE_ATTRIBUTE.dat', header 'true', inferSchema 'true', nullValue 'NULL', maxCharsPerColumn '4096');
  CREATE TABLE CLAIM_LINE_ATTRIBUTE USING column OPTIONS(partition_by 'PRSN_ID', buckets '32',key_columns 'CLIENT_ID,PRSN_ID,CLM_DTL_ID,CLM_LN_ATTR_ID ' ) AS (SELECT * FROM staging_CLAIM_LINE_ATTRIBUTE);

CREATE EXTERNAL TABLE staging_CLAIM_PAYMENT_REDUCTION
    USING com.databricks.spark.csv OPTIONS (path '/export/shared/QA_DATA/USER2_Data_20G/CLAIM_PAYMENT_REDUCTION.dat', header 'true', inferSchema 'true', nullValue 'NULL', maxCharsPerColumn '4096');
CREATE TABLE CLAIM_PAYMENT_REDUCTION USING column OPTIONS(partition_by 'CLM_PAY_RDCTN_ID', buckets '32',key_columns 'CLIENT_ID,CLM_PAY_RDCTN_ID ' ) AS (SELECT * FROM staging_CLAIM_PAYMENT_REDUCTION);

CREATE EXTERNAL TABLE staging_CLAIM_REDUCTION_DETAIL
    USING com.databricks.spark.csv OPTIONS (path '/export/shared/QA_DATA/USER2_Data_20G/CLAIM_REDUCTION_DETAIL.dat', header 'true', inferSchema 'true', nullValue 'NULL', maxCharsPerColumn '4096');
CREATE TABLE CLAIM_REDUCTION_DETAIL USING column OPTIONS(partition_by 'PRSN_ID',buckets '32',key_columns 'CLIENT_ID,PRSN_ID,CLM_ID,CLM_PAY_RDCTN_ID,CLM_RDCTN_DTL_ID ' ) AS (SELECT * FROM staging_CLAIM_REDUCTION_DETAIL);

CREATE EXTERNAL TABLE staging_CLAIM_REDUCTION_HISTORY
    USING com.databricks.spark.csv OPTIONS (path '/export/shared/QA_DATA/USER2_Data_20G/CLAIM_REDUCTION_HISTORY.dat', header 'true', inferSchema 'true', nullValue 'NULL', maxCharsPerColumn '4096');
CREATE TABLE CLAIM_REDUCTION_HISTORY USING column OPTIONS(partition_by 'CLM_PAY_RDCTN_ID', buckets '32',key_columns 'CLIENT_ID,CLM_PAY_RDCTN_ID,CLM_RDCTN_HIST_ID ' ) AS (SELECT * FROM staging_CLAIM_REDUCTION_HISTORY);

CREATE EXTERNAL TABLE staging_CLIENT_REFERENCE_DATA
    USING com.databricks.spark.csv OPTIONS (path '/export/shared/QA_DATA/USER2_Data_20G/CLIENT_REFERENCE_DATA.dat', header 'true', inferSchema 'true', nullValue 'NULL', maxCharsPerColumn '4096');
CREATE TABLE CLIENT_REFERENCE_DATA USING column OPTIONS(partition_by 'CLIENT_REF_DTA_ID', buckets '32',key_columns 'CLIENT_ID,CLIENT_REF_DTA_ID ' ) AS (SELECT * FROM staging_CLIENT_REFERENCE_DATA);

CREATE EXTERNAL TABLE staging_CLIENTS
    USING com.databricks.spark.csv OPTIONS (path '/export/shared/QA_DATA/USER2_Data_20G/CLIENTS.dat', header 'true', inferSchema 'true', nullValue 'NULL', maxCharsPerColumn '4096');
CREATE TABLE CLIENTS USING column OPTIONS(partition_by 'CLIENT_ID', buckets '32',key_columns 'CLIENT_ID ' ) AS (SELECT * FROM staging_CLIENTS);

CREATE EXTERNAL TABLE staging_COB_CLAIM_DIAGNOSIS
    USING com.databricks.spark.csv OPTIONS (path '/export/shared/QA_DATA/USER2_Data_20G/COB_CLAIM_DIAGNOSIS.dat', header 'true', inferSchema 'true', nullValue 'NULL', maxCharsPerColumn '4096');
CREATE TABLE COB_CLAIM_DIAGNOSIS USING column OPTIONS(partition_by 'PRSN_ID',buckets '32',key_columns 'CLIENT_ID,PRSN_ID,PRSN_COB_ID,REC_ORD ' ) AS (SELECT * FROM staging_COB_CLAIM_DIAGNOSIS);

CREATE EXTERNAL TABLE staging_COB_ORGANIZATION_PERSON
    USING com.databricks.spark.csv OPTIONS (path '/export/shared/QA_DATA/USER2_Data_20G/COB_ORGANIZATION_PERSON.dat', header 'true', inferSchema 'true', nullValue 'NULL', maxCharsPerColumn '4096');
CREATE TABLE COB_ORGANIZATION_PERSON USING column OPTIONS(partition_by 'PRSN_ID',buckets '32',key_columns 'CLIENT_ID,PRSN_ID,PRSN_COB_ID,ORG_PRSN_TYP_REF_ID ' ) AS (SELECT * FROM staging_COB_ORGANIZATION_PERSON);

