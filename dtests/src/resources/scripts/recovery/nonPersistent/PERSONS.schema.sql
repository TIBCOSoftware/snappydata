create diskstore DISK_STORE;

Create table IF NOT EXISTS ODS.PERSONS(
      prsn_id bigint NOT NULL,
      ver bigint NOT NULL,
      client_id bigint NOT NULL,
      ttl LONG VARCHAR,
      first_nm LONG VARCHAR,
      last_nm LONG VARCHAR,
      mid_nm LONG VARCHAR,
      sfx LONG VARCHAR,
      birth_dt DATE,
      gndr_ref_id bigint,
      ethn_ref_id bigint,
      mar_stat_ref_id bigint,
      lang_ref_id bigint,
      prsn_stat_ref_id bigint,
      eff_dt DATE,
      expr_dt DATE,
      attach_src_id bigint,
      vld_frm_dt TIMESTAMP NOT NULL,
      vld_to_dt TIMESTAMP,
      src_sys_ref_id LONG VARCHAR NOT NULL,
      src_sys_rec_id LONG VARCHAR NOT NULL,
      PRIMARY KEY (client_id,prsn_id)
      )
      USING row OPTIONS(partition_by 'prsn_id', PERSISTENCE 'NONE', REDUNDANCY '1', EVICTION_BY 'LRUHEAPPERCENT', OVERFLOW 'true', diskstore 'DISK_STORE');

create index ODS.IX_PERSONS_02 on ODS.PERSONS (CLIENT_ID,FIRST_NM);
create index ODS.IX_PERSONS_03 on ODS.PERSONS (CLIENT_ID,LAST_NM);
create index ODS.IX_PERSONS_04 on ODS.PERSONS (CLIENT_ID,BIRTH_DT);
