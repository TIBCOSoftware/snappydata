DROP TABLE IF EXISTS AGREEMENT;
CREATE TABLE AGREEMENT( AGREE_ID BIGINT NOT NULL,
    VER BIGINT NOT NULL,
    CLIENT_ID BIGINT NOT NULL,
    AGREE_CD VARCHAR(200),
    DESCR VARCHAR(200),
    EFF_DT DATE,
    EXPR_DT DATE,
    VLD_FRM_DT TIMESTAMP NOT NULL,
    VLD_TO_DT TIMESTAMP,
    SRC_SYS_REF_ID VARCHAR(200) NOT NULL,
    SRC_SYS_REC_ID VARCHAR(200)) USING column OPTIONS(partition_by 'AGREE_ID', buckets '32',key_columns 'CLIENT_ID,AGREE_ID ',redundancy '1' );
    INSERT into AGREEMENT select id,abs(rand()*1000),abs(rand()*1000),'agree_cd','description','2018-01-01','2019-01-01',from_unixtime(unix_timestamp('2018-01-01 01:00:00')+floor(rand()*31536000)),from_unixtime(unix_timestamp('2019-01-01 01:00:00')+floor(rand()*31536000)),'src_sys_ref_id','src_sys_rec_id' FROM range(50500);

DROP TABLE IF EXISTS AGREEMENT_ROW;
CREATE TABLE AGREEMENT_ROW( AGREE_ID BIGINT NOT NULL,
    VER BIGINT NOT NULL,
    CLIENT_ID BIGINT NOT NULL,
    AGREE_CD VARCHAR(200),
    DESCR VARCHAR(200),
    EFF_DT DATE,
    EXPR_DT DATE,
    VLD_FRM_DT TIMESTAMP NOT NULL,
    VLD_TO_DT TIMESTAMP,
    SRC_SYS_REF_ID VARCHAR(200) NOT NULL,
    SRC_SYS_REC_ID VARCHAR(200)) USING row OPTIONS(partition_by 'AGREE_ID', buckets '32',redundancy '1' );
    INSERT into AGREEMENT_ROW select id,abs(rand()*1000),abs(rand()*1000),'agree_cd','description','2018-01-01','2019-01-01',from_unixtime(unix_timestamp('2018-01-01 01:00:00')+floor(rand()*31536000)),from_unixtime(unix_timestamp('2019-01-01 01:00:00')+floor(rand()*31536000)),'src_sys_ref_id','src_sys_rec_id' FROM range(50500);

DROP TABLE IF EXISTS AGREEMENT_RR;
CREATE TABLE AGREEMENT_RR( AGREE_ID BIGINT NOT NULL,
  VER BIGINT NOT NULL,
  CLIENT_ID BIGINT NOT NULL,
  AGREE_CD VARCHAR(200),
  DESCR VARCHAR(200),
  EFF_DT DATE,
  EXPR_DT DATE,
  VLD_FRM_DT TIMESTAMP NOT NULL,
  VLD_TO_DT TIMESTAMP,
  SRC_SYS_REF_ID VARCHAR(200) NOT NULL,
  SRC_SYS_REC_ID VARCHAR(200)) ;
  INSERT into AGREEMENT_RR select id,abs(rand()*1000),abs(rand()*1000),'agree_cd','description','2018-01-01','2019-01-01',from_unixtime(unix_timestamp('2018-01-01 01:00:00')+floor(rand()*31536000)),from_unixtime(unix_timestamp('2019-01-01 01:00:00')+floor(rand()*31536000)),'src_sys_ref_id','src_sys_rec_id' FROM range(10000);

DROP TABLE IF EXISTS BANK;
CREATE TABLE BANK(
  BNK_ORG_ID BIGINT NOT NULL,
  BNK_ID BIGINT NOT NULL,
  VER BIGINT NOT NULL,
  CLIENT_ID BIGINT NOT NULL,
  BNK_FULL_NM VARCHAR(50),
  RTNG_NUM VARCHAR(35) NOT NULL,
  VLD_FRM_DT TIMESTAMP NOT NULL,
  VLD_TO_DT TIMESTAMP,
  SRC_SYS_REF_ID VARCHAR(50) NOT NULL,
  SRC_SYS_REC_ID VARCHAR(150)) USING column OPTIONS(partition_by 'BNK_ORG_ID', buckets '32',key_columns 'CLIENT_ID,BNK_ORG_ID,BNK_ID ',redundancy '1') ;
  INSERT into BANK select id,id,abs(rand()*1000),abs(rand()*1000),'BNK_FULL_NM','RTNG_NUM',from_unixtime(unix_timestamp('2018-01-01 01:00:00')+floor(rand()*31536000)),from_unixtime(unix_timestamp('2019-01-01 01:00:00')+floor(rand()*31536000)),'src_sys_ref_id','src_sys_rec_id' from range(50500);

 CREATE TABLE t1 (col int,col2 int) USING COLUMN options(redundancy '1');
 CREATE TABLE t2 (col int,col2 int) USING COLUMN options(redundancy '1');
 CREATE TABLE t3 (col int,col2 int) USING COLUMN options(redundancy '1');
