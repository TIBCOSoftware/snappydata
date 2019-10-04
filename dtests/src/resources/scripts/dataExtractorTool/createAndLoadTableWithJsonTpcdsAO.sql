DROP TABLE IF EXISTS testL1;
create table testL1 (id string NOT NULL, data string, data2 decimal, APPLICATION_ID string NOT NULL, ORDERGROUPID string,
 PAYMENTADDRESS1 string, PAYMENTADDRESS2 string, PAYMENTCOUNTRY string, PAYMENTSTATUS string, PAYMENTRESULT string,
 PAYMENTZIP string, PAYMENTSETUP string, PROVIDER_RESPONSE_DETAILS string, PAYMENTAMOUNT string, PAYMENTCHANNEL string,
 PAYMENTCITY string, PAYMENTSTATECODE string,PAYMENTSETDOWN string,  PAYMENTREFNUMBER string, PAYMENTST string,
 PAYMENTAUTHCODE string,  PAYMENTID string, PAYMENTMERCHID string, PAYMENTHOSTRESPONSECODE string,PAYMENTNAME string,
 PAYMENTOUTLETID string, PAYMENTTRANSTYPE string,  PAYMENTDATE string, CLIENT_ID string, CUSTOMERID string)
using column options (partition_by 'id,APPLICATION_ID',COLUMN_MAX_DELTA_ROWS '1000',overflow 'true',key_columns 'id,APPLICATION_ID',redundancy '1', BUCKETS '96');

DROP TABLE IF EXISTS testL2;
create table testL2 (id string NOT NULL, data string, data2 decimal, APPLICATION_ID string NOT NULL, ORDERGROUPID string,
 PAYMENTADDRESS1 string, PAYMENTADDRESS2 string, PAYMENTCOUNTRY string, PAYMENTSTATUS string, PAYMENTRESULT string,
 PAYMENTZIP string, PAYMENTSETUP string, PROVIDER_RESPONSE_DETAILS string, PAYMENTAMOUNT string, PAYMENTCHANNEL string,
 PAYMENTCITY string, PAYMENTSTATECODE string,PAYMENTSETDOWN string,  PAYMENTREFNUMBER string, PAYMENTST string,
 PAYMENTAUTHCODE string,  PAYMENTID string, PAYMENTMERCHID string, PAYMENTHOSTRESPONSECODE string,PAYMENTNAME string,
 PAYMENTOUTLETID string, PAYMENTTRANSTYPE string,  PAYMENTDATE string, CLIENT_ID string, CUSTOMERID string)
using column options (partition_by 'id,APPLICATION_ID',COLUMN_MAX_DELTA_ROWS '1000',overflow 'true',key_columns 'id,APPLICATION_ID',redundancy '1', BUCKETS '96');

DROP TABLE IF EXISTS testL3;
create table testL3 (id string NOT NULL, data string, data2 decimal, APPLICATION_ID string NOT NULL, ORDERGROUPID string,
 PAYMENTADDRESS1 string, PAYMENTADDRESS2 string, PAYMENTCOUNTRY string, PAYMENTSTATUS string, PAYMENTRESULT string,
 PAYMENTZIP string, PAYMENTSETUP string, PROVIDER_RESPONSE_DETAILS string, PAYMENTAMOUNT string, PAYMENTCHANNEL string,
 PAYMENTCITY string, PAYMENTSTATECODE string,PAYMENTSETDOWN string,  PAYMENTREFNUMBER string, PAYMENTST string,
 PAYMENTAUTHCODE string,  PAYMENTID string, PAYMENTMERCHID string, PAYMENTHOSTRESPONSECODE string,PAYMENTNAME string,
 PAYMENTOUTLETID string, PAYMENTTRANSTYPE string,  PAYMENTDATE string, CLIENT_ID string, CUSTOMERID string)
using column options (partition_by 'id,APPLICATION_ID',COLUMN_MAX_DELTA_ROWS '1000',overflow 'true',key_columns 'id,APPLICATION_ID',redundancy '1', BUCKETS '96');

create external table climateChange_staging USING com.databricks.spark.csv OPTIONS(path ':dataLocation/climate1788-2011.csv', header 'true', inferSchema 'true', nullValue 'NULL', maxCharsPerColumn '4096');
create table climateChange Using column options(buckets '32',redundancy '1') AS (select * from climateChange_staging);

Drop table if exists solution_dm;
create table if not exists solution_dm(solution_key BIGINT,solution_id VARCHAR(50),name VARCHAR(50),version VARCHAR(50)) USING ROW;
Drop table if exists role_dm;
create table if not exists role_dm(role_key BIGINT,name VARCHAR(50)) USING ROW;
Drop table if exists city_dm;
create table if not exists city_dm(city_key BIGINT,county_key BIGINT,state_key BIGINT,country_key BIGINT, name VARCHAR(50),location_geometry varchar(200),boundary_geometry varchar(200)) USING ROW;
Drop table if exists county_dm;
create table if not exists county_dm(county_key BIGINT,state_key BIGINT,country_key BIGINT, name VARCHAR(50),location_geometry varchar(200),boundary_geometry varchar(200)) USING ROW;
Drop table if exists state_dm;
create table if not exists state_dm(state_key BIGINT,country_key BIGINT,code VARCHAR(50),name VARCHAR(50),location_geometry varchar(200),boundary_geometry varchar(200)) USING ROW;
Drop table if exists country_dm;
create table if not exists country_dm(country_key BIGINT,code VARCHAR(50), name VARCHAR(50),location_geometry varchar(200),boundary_geometry varchar(200)) USING ROW;
Drop table if exists continent_dm;
create table if not exists continent_dm(continent_key BIGINT,continent_name VARCHAR(50),sub_continent_name VARCHAR(50), user_key BIGINT) USING ROW;
Drop table if exists user_agent_dm;
create table if not exists user_agent_dm(user_agent_key BIGINT,type VARCHAR(50) ,family VARCHAR(50) ,major_version VARCHAR(50), IS_MOBILE BOOLEAN) USING ROW;
Drop table if exists screen_resolution_dm;
create table if not exists screen_resolution_dm(scr_res_key BIGINT,resolution VARCHAR(50),screen_colors VARCHAR(50)) USING ROW;
Drop table if exists operating_system_dm;
create table if not exists operating_system_dm(os_key BIGINT,name VARCHAR(50),family VARCHAR(50) ,major_version VARCHAR(50) ) USING ROW;
Drop table if exists device_dm;
create table if not exists device_dm(device_key BIGINT,name VARCHAR(50),icon VARCHAR(50), family VARCHAR(50),DEVICE_INFO_URL VARCHAR (50)) USING ROW;
Drop table if exists language_dm;
create table if not exists language_dm(language_key BIGINT,code VARCHAR(50),name VARCHAR(50)) USING ROW;
Drop table if exists network_provider_dm;
create table if not exists network_provider_dm(network_provider_key BIGINT,provider_name VARCHAR(50)) USING ROW;
Drop table if exists application_dm;
create table if not exists application_dm(application_key BIGINT,appid varchar(100),appname varchar(100), version VARCHAR(50)) USING ROW;
Drop table if exists event_dm;
create table if not exists event_dm(event_key BIGINT,event_name VARCHAR(50),event_type VARCHAR(50)) USING ROW;
Drop table if exists exception_dm;
create table if not exists exception_dm(exception_key BIGINT,exception_code VARCHAR(50), exception_description VARCHAR(50), exception_type VARCHAR(50), severity VARCHAR(50)) USING ROW;
Drop table if exists solution_page_dm;
create table if not exists solution_page_dm(solution_page_key BIGINT,solution_key BIGINT,solution_page_id VARCHAR(50)) USING ROW;
Drop table if exists user_session_ft;
create table if not exists user_session_ft(user_key BIGINT,session_id VARCHAR(50),token_id VARCHAR(1000),login_date TIMESTAMP,logout_date TIMESTAMP,event_time TIMESTAMP,job_id BIGINT) USING COLUMN OPTIONS(PARTITION_BY 'event_time',buckets '96');
Drop table if exists solution_page_ft;
create table if not exists solution_page_ft(user_key BIGINT,solution_key   BIGINT,solution_page_key BIGINT,session_id VARCHAR(50),token_id VARCHAR(1000),referral_solution_page_key BIGINT,solution_pageload_time NUMERIC,event_time TIMESTAMP,page_title varchar(100),job_id BIGINT)
USING COLUMN OPTIONS(PARTITION_BY 'event_time',buckets '96');
Drop table if exists solution_app_event_ft;
create table if not exists solution_app_event_ft(solution_key BIGINT,application_key BIGINT,solution_page_key BIGINT,user_key BIGINT,user_agent_key BIGINT,os_key BIGINT,device_key BIGINT,city_key BIGINT,county_key BIGINT,state_key BIGINT,country_key BIGINT,session_id VARCHAR(50),token_id VARCHAR(1000),action_name VARCHAR(50),event_key BIGINT,action_value VARCHAR(50),event_time   TIMESTAMP,referral_app_key BIGINT,LOAD_TIME BIGINT,TIME_SPENT BIGINT,job_id BIGINT) USING COLUMN OPTIONS(PARTITION_BY 'event_time',buckets '96');
Drop table if exists user_agent_geography_ft;
create table if not exists user_agent_geography_ft(session_id VARCHAR(50),token_id VARCHAR(1000),scr_res_key BIGINT,solution_key BIGINT,user_agent_key BIGINT,device_key BIGINT,os_key BIGINT,language_key BIGINT,network_provider_key BIGINT,user_key BIGINT,city_key BIGINT ,county_key BIGINT,state_key BIGINT,country_key BIGINT,ip_address VARCHAR(50),longitude   VARCHAR(50),latitude VARCHAR(50),network_domain_name VARCHAR(50),event_time TIMESTAMP,user_agent_raw_text CLOB,job_id BIGINT)USING COLUMN OPTIONS(PARTITION_BY   'event_time',buckets '96');
Drop table if exists solution_search_ft;
create table if not exists solution_search_ft(solution_key BIGINT,application_key BIGINT,user_key BIGINT,solution_page_key BIGINT,session_id VARCHAR(50),token_id VARCHAR(1000),search_term VARCHAR(500),refined_search_term VARCHAR(500),sort_parameters VARCHAR(500),search_category   VARCHAR(500),search_result_count BIGINT,event_time TIMESTAMP,EVENT_KEY BIGINT,voice_search BOOLEAN,SELECTION_MATCH VARCHAR(500),SELECTED_VALUE VARCHAR(500),SEARCH_TIME BIGINT,QUERY_FETCH_TIME BIGINT,job_id BIGINT) USING COLUMN OPTIONS(PARTITION_BY   'event_time',buckets '96');
Drop table if exists job_dm;
create table if not exists job_dm(
   job_key bigint,
   code varchar(100),
   title varchar(100),
   family varchar(100),
   job_role varchar(100),
   region varchar(100));
Drop table if exists organization_dm;
create table if not exists organization_dm(
   organization_key bigint,
   code varchar(100),
   name varchar(50),
   description varchar(100));
Drop table if exists division_dm;
create table if not exists division_dm(
   division_key bigint,
   organization_key BIGINT,
   name varchar(100));
Drop table if exists department_dm;
create table if not exists department_dm(
   department_key bigint,
   id varchar(100),
   name varchar(100),
   organization_key BIGINT,
   division_key BIGINT);
Drop table if exists User_DM;
create table if not exists User_DM(
      user_key bigint,
      department_key bigint,
      division_key BIGINT,
      organization_key bigint,
      job_key bigint,
      user_id varchar(100),
      full_name varchar(100),
      location varchar(100),
      is_manager boolean,
      is_active boolean);
Drop table if exists solution_exception_ft;
create table if not exists solution_exception_ft(solution_key BIGINT,solution_page_key BIGINT,application_key BIGINT,exception_key BIGINT,event_time TIMESTAMP,job_id BIGINT) USING COLUMN OPTIONS(PARTITION_BY 'solution_key,solution_page_key,application_key,exception_key');
Drop table if exists solution_c1v_ft;
create table if not exists solution_c1v_ft(solution_key bigint ,application_key BIGINT ,event_time TIMESTAMP ,event_key BIGINT ,user_key BIGINT,balance_forward DOUBLE ,bill_id VARCHAR(500) ,
business_partner_number VARCHAR(500) ,connection_contract_number VARCHAR(500) ,contact_activity VARCHAR(500) ,contact_class VARCHAR(500) ,contacted_count VARCHAR(500) ,customer_id VARCHAR(500) ,data_toggle_type VARCHAR(500) ,device_location_number VARCHAR(500) ,
displayed_meter VARCHAR(500) ,document_id VARCHAR(500) ,duration_in_months LONG ,end_month VARCHAR(500) ,estimated_bill_count INTEGER ,estimated_bill_flag VARCHAR(500) ,favourite_count INTEGER ,high_bill_count INTEGER ,
high_bill_flag VARCHAR(500) ,high_bill_months CLOB ,interaction_id VARCHAR(500) ,late_payment_count integer ,
late_payment_months CLOB ,meter_id CLOB ,ontime_payments_count INTEGER ,outstanding_balance DOUBLE ,
payment_method VARCHAR(500) ,premise_id VARCHAR(500) ,premise_number VARCHAR(500) ,
rebill_count INTEGER ,rebill_flag VARCHAR(500) ,readings_start_month VARCHAR(500) ,
readings_end_month VARCHAR(500) ,recent_count INTEGER ,serial_number CLOB ,sort_by VARCHAR(500) ,
sort_parameters CLOB ,start_month VARCHAR(500) ,straight_to_result VARCHAR(500) ,status VARCHAR(500) ,
emailAddress_emailAddress varchar(100),emailAddress_emailCategory varchar(100),emailAddress_preference_preferred varchar(100),
additionalProperties CLOB,favouriteBusinessPartnerNumbers CLOB,phone_phoneType varchar(500),phone_preference_preferred varchar(100),
phone_preference_preferenceType varchar(100),geoAddress CLOB,userInformation_userId varchar(100),description varchar(500),
securityLevel varchar(100),userInformation_location varchar(500),phone_phoneCategory varchar(100),userInformation_role varchar(100),
postalCode varchar(10),mapViewType varchar(100),voice varchar(100),
totalCount BIGINT,action_name varchar(100),warranty_expiry_year CLOB,job_id BIGINT ) USING COLUMN OPTIONS(PARTITION_BY 'event_time',buckets '96');
Drop table if exists SOLUTION_METEROPS_FT;
CREATE TABLE IF NOT EXISTS SOLUTION_METEROPS_FT(APPLICATION_KEY BIGINT,SOLUTION_KEY BIGINT, EVENT_KEY BIGINT, USER_KEY BIGINT, TREND_DESCRIPTION VARCHAR(100),TYPE_CODE STRING,
   METHOD_NAME STRING,RATING_VALUE VARCHAR(100),POSITION LONG,BILLS_CONSUMPTION STRING,BASKET_NAME STRING,REGISTER VARCHAR(100),MESSAGE VARCHAR(100), BASKET_DETAILS_BASKET_NAME VARCHAR(100),
   REVIEW VARCHAR(100),ACCESS_MODE VARCHAR(100),BILLS_DUE_DATE VARCHAR(100),BILLS_REBILL_FLAG VARCHAR(100), ON_TREND   VARCHAR(100), CONNECTION_CONTRACT_NUMBER LONG,
   EVENT_TIME TIMESTAMP, SECURITY_LEVEL VARCHAR(100),BILLS_HIGH_BILL_FLAG VARCHAR(100),STRAIGHT_TO_RESULT VARCHAR(100), BILLS_BILL_MONTH VARCHAR(100),
   NOTE_SUBTYPE VARCHAR(100),BILLS_BALANCE_FORWARD VARCHAR(100),BILLS_CURRENT_CHARGES VARCHAR(100),BASKET_METER_COUNT LONG,BUSINESS_PARTNER_NUMBER LONG,
   TENANT_ID INTEGER,NOTE_TYPE VARCHAR(100),METER_ID LONG,BASKET_DETAILS_BASKET_METER_COUNT VARCHAR(100),CUSTOMER_NUMBER VARCHAR(100),
   ACCESS_METHOD VARCHAR(100),WORKORDER_CREATED_COUNT VARCHAR(100),SERIAL_NUMBER VARCHAR(100),PREFERENCES_APPLIED BOOLEAN,
   SERVICE_ORDER_NUMBER VARCHAR(100),ESTIMATED_BILL_FLAG VARCHAR(100),TREND_COUNT INTEGER,TREND VARCHAR(100),PREFERENCE_APPLIED BOOLEAN,
   BILLS_STATEMENT_DATE VARCHAR(100),TREND_CODE VARCHAR(100),CLASS_NAME VARCHAR(100),PREFERENCES STRING,BILLS_PAID_FLAG VARCHAR(100),JOB_ID BIGINT) USING COLUMN OPTIONS(PARTITION_BY 'event_time',buckets '96');

Drop table if exists usage_analytics_job_table;
create table if not exists usage_analytics_job_table(
                  ID BIGINT,
                  JOB_NAME VARCHAR(250),
                  ES_DELTALOAD_START_TIME TIMESTAMP,
                  ES_DELTALOAD_END_TIME TIMESTAMP,
                  JOB_START_TIME TIMESTAMP,
                  JOB_END_TIME TIMESTAMP,
                  DURATION BIGINT,
                  STATUS VARCHAR(20));

Drop table if exists usage_analytics_pipeline_job_table;
create table if not exists usage_analytics_pipeline_job_table(
                  ID BIGINT,
                  JOB_Id BIGINT,
                  PIPELINE_NAME VARCHAR(50),
                  STATUS VARCHAR(20),
                  SOLUTION_ID VARCHAR(40),
                  source_index varchar(20));

Drop table if exists SOLUTION_CONFIG;
create table if not exists SOLUTION_CONFIG(
                  SOLUTION_NAME VARCHAR(250),
                  HISTORICAL_INDEXES STRING,
                  FIELDS STRING,
                  IS_BUSINESS_TABLE_REQUIRED BOOLEAN);
Drop table if exists MISSING_EVENT_DATES;
create table if not exists MISSING_EVENT_DATES(SOLUTION_ID VARCHAR(50),EVENT_DATE DATE) USING COLUMN OPTIONS(BUCKETS '64');

insert into solution_dm SELECT (id%4),'solution_id'||(id%4),'solutionName'||id,'version'||id FROM range(4);

insert into role_dm SELECT id,'name'||id FROM range(1000);

insert into city_dm SELECT id,abs(randn()*10000000000000) ,abs(randn()*10000000000000),abs(randn()*10000000000000),'name','location_geometry'||id,'boundary_geometry'||id FROM range(404);

insert into  county_dm SELECT id,abs(randn()*10000000000000),abs(randn()*10000000000000),'name','location_geometry'||id,'boundary_geometry'||id FROM range(195);

insert into state_dm SELECT id,abs(randn()*10000000000000),'code'||id,'name','location_geometry'||id,'boundary_geometry'||id FROM range(28);

insert into country_dm SELECT id,'code'||id,'name','location_geometry'||id,'boundary_geometry'||id FROM range(1);

insert into continent_dm SELECT id,'continent_name'||id,'sub_continent_name'||id,abs(randn()*10000000000000) FROM range(1000);

insert into user_agent_dm SELECT id,'type'||id,'family'||(round(abs(randn()*100000)%11)),'major_version'||id,'1' FROM range(58);

insert into screen_resolution_dm SELECT id,'resolution'||id,'screen_colors'||id FROM range(1000);

insert into operating_system_dm SELECT (abs(randn()*10000000000000)%21),'name','family'||(round(abs(randn()*100000)%6)),'major_version'||id FROM range(21);

insert into device_dm SELECT id,'name','icon','family'||(round(abs(randn()*100000)%4)),'DEVICE_INFO_URL'||id FROM range(14);

insert into language_dm SELECT id,'code'||id,'name' FROM range(1000);

insert into network_provider_dm SELECT id,'provider_name'||id FROM range(1000);

insert into application_dm SELECT id,'appid'||id,'app_name','version'||id FROM range(107);

insert into event_dm SELECT id,'event_name','event_type' FROM range(34);

insert into exception_dm SELECT id,'exception_code'||id,'exceptio_description'||id,'exception_type_'||id,'severity_'||id FROM range(1000);

insert into solution_page_dm SELECT id,abs(randn()*10000),'solution_page_id'||id FROM range(38);

insert into user_session_ft SELECT id,'session_id'||id,'token_id'||id,from_unixtime(unix_timestamp('2018-01-01 01:00:00')+floor(rand()*31536000)),from_unixtime(unix_timestamp('2019-01-01 01:00:00')+floor(rand()*31536000)),from_unixtime(unix_timestamp('2018-01-01 01:00:00')+floor(rand()*31536000)),abs(randn()*10000000000000) FROM range(737967);

--1.20 GB
insert into solution_page_ft SELECT id,abs(randn()*10000000000000),abs(randn()*10000000000000),lpad('session_id',50,rand()),lpad('token_id',1000,'somerandom'),abs(randn()*10000000000000),id,from_unixtime(unix_timestamp('2018-01-01 01:00:00')+floor(rand()*31536000)),lpad('page_title',100,rand()),abs(randn()*10000000000000) FROM range(6666666);

--4.72 GB %1440289
insert into solution_app_event_ft SELECT (id%4),abs(randn()*10000000000000),abs(randn()*10000000000000),abs(randn()*10000000000000),id,abs(randn()*10000000000000)%16,id,abs(randn()*10000000000000),abs(randn()*10000000000000),abs(randn()*10000000000000),abs(randn()*10000000000000),lpad('session_id',50,rand()) ,lpad('token_id',1000,(round(abs(randn()*100000))%88031)),'action_name',id,lpad('action_value',50,rand()),timestamp(date_sub(current_timestamp(),int(rand()*300))),abs(randn()*10000000000000),abs(randn()*10000000000000),abs(randn()*10000000000000),abs(randn()*10000000000000) FROM range(5184251);

insert into user_agent_geography_ft SELECT 'session_id'||id ,'token_id'||id,abs(randn()*10000000000000),abs(randn()*10000000000000),abs(randn()*10000000000000),abs(randn()*10000000000000),abs(randn()*10000000000000),abs(randn()*10000000000000),abs(randn()*10000000000000),abs(randn()*10000000000000),abs(randn()*10000000000000),abs(randn()*10000000000000),abs(randn()*10000000000000),abs(randn()*10000000000000),'ip_address_'||id,'longitude','latitude','network_domain_name',from_unixtime(unix_timestamp('2018-01-01 01:00:00')+floor(rand()*31536000))   ,'user_agent_raw_text',abs(randn()*10000000000000) FROM range(746083);

insert into solution_search_ft SELECT abs(randn()*10000000000000),abs(randn()*10000000000000),abs(randn()*10000000000000),abs(randn()*10000000000000),'session_id'||id,'token_id'||id,'search_term'||id,'refined_search_term'||id,'sort_parameters'||id,'search_category'||id,abs(randn()*10000000000000),from_unixtime(unix_timestamp('2018-01-01 01:00:00')+floor(rand()*31536000)),abs(randn()*10000000000000),'1','selected_match'||id,'Selected_values'||id,abs(randn()*10000000000000),abs(randn()*10000000000000),abs(randn()*10000000000000) FROM range(862874);

insert into job_dm SELECT abs(randn()*10000),'code'||id,'title'||id,'family'||(round(abs(randn()*100000)%14)),'job_role','region'||id FROm range(136);

insert into organization_dm SELECT abs(randn()*10000),'code','name','description' FROM range(25);

insert into division_dm SELECT abs(randn()*10000),abs(randn()*10000),'name'||id FROM range(43);

insert into department_dm SELECT abs(randn()*10000),'id'||id,'name'||id,abs(randn()*10000),abs(randn()*10000) FROM range(360);

insert into User_DM SELECT abs(randn()*10000),abs(randn()*10000),abs(randn()*10000),abs(randn()*10000),abs(randn()*10000),'user_id'||id,'full_name'||id,'location'||id,'1','0' FROM range(7774);

insert into solution_exception_ft SELECT abs(randn()*10000),abs(randn()*10000),abs(randn()*10000),abs(randn()*10000),from_unixtime(unix_timestamp('2018-01-01 01:00:00')+floor(rand()*31536000))   ,abs(randn()*10000) FROM range(1000);

insert into solution_c1v_ft SELECT abs(randn()*10000),abs(randn()*10000),from_unixtime(unix_timestamp('2018-01-01 01:00:00')+floor(rand()*31536000))   ,abs(randn()*10000),abs(randn()*10000),round(rand(),2),'bill_id'||id,
'bussiness_patner_number','connection_contact_number','contact_activity','contaact_class','contacted_count','customer_id','data_toggle_type','device_location_number',
'displayed_meter','document_id'||id,id,'end_of_month',id,'estimated_bill_flag',id,id,'high_bill_flag',
'high_bill_months','interaction_id'||id,abs(randn()*1000),'late_payment_months','meter_id'||id,abs(randn()*1000),round(rand(),2),
'payment_method','premise_id','premise_number',
abs(randn()*1000),'rebill_flag','readings_start_month',
'reading_end-month',abs(randn()*1000),'serial_number','sort_by'||id,
'sort_parameters','start_month'||id,'straight_to_result','status',
'emailAddress_emailAddress'||id,'emailAddress_emailCategory'||id,'emailAddress_preference_preferred',
'additionalProperties','favouriteBusinessPartnerNumbers','phone_phoneType','phone_preference_preferred',
'phone_preference_preferenceType','geoAddress','userInformation_userId','description',
'security_level','userInformation_location','phone_phoneCategory','userInformation_role',
'4100248','mapViewType','voice',
abs(randn()*10000),'action_name','warranty_expiry_year',abs(randn()*10000) FROM range(4948150);

insert into SOLUTION_METEROPS_FT SELECT id,abs(randn()*10000),abs(randn()*10000),abs(randn()*10000),'TREND_DESCRIPTION','TYPE_CODE',
'METHID_NAME','RATING_VALUE',id,'BILLD_CONSUPTION','BASKET_NAME','REGISTER'||id,'MESSAGE','BASKET_DETAILS_BASKET_NAME ',
'REVIEW','ACCESS_MODE','BILLS_DUE_DATE','BILLS_REBILL_FLAG','ON_TREND',id,
from_unixtime(unix_timestamp('2018-01-01 01:00:00')+floor(rand()*31536000))   ,'SECURITY_LEVEL','BILLS_HIGH_BILL_FLAG','STRAIGHT_TO_RESULT','BILLS_BILL_MONTH',
'NOTE_SUBTYPE','BILLS_BALANCE_FORWARD','BILLS_CURRENT_CHARGES',id,abs(randn()*100000),
abs(randn()*1000),'NOTE_TYPE',abs(randn()*10000),'BASKET_DETAILS_BASKET_METER_COUNT','CUSTOMER_NUMBER_'||id,
'ACCESS_METHOD','WORKORDER_CREATED_COUNT','SERIAL_NUMBER','1',
'SERVICE_ORDER_NUMBER','ESTIMATED_BILL_FLAG',abs(randn()*1000),'TREND','0',
'BILLS_STATEMENT_DATE','TREND_CODE','CLASS_NAME','PREFERENCES','BILLS_PAID_FLAG',abs(randn()*1000) FROM range(323487);

insert into usage_analytics_job_table SELECT id,'JOB_NAME',from_unixtime(unix_timestamp('2018-01-01 01:00:00')+floor(rand()*31536000)),from_unixtime(unix_timestamp('2019-01-01 01:00:00')+floor(rand()*31536000))   ,from_unixtime(unix_timestamp('2018-01-01 01:00:00')+floor(rand()*31536000))   ,from_unixtime(unix_timestamp('2019-01-01 01:00:00')+floor(rand()*31536000))   ,id,'COMPLETE' FROM range(1000);

insert into usage_analytics_pipeline_job_table SELECT id,abs(randn()*1000),'PIPELINE_NAME','STATUS','SOLUTION_ID'||id,'source_index' FROM range(1000);

insert into SOLUTION_CONFIG SELECT 'SOLUTION_NAME_'||id,'HISTORICAL_INDEXES','FILEDS','1' FROM range(1000);

insert into MISSING_EVENT_DATES SELECT  'SOLUTION_NAME_'||id,current_date() FROM range(788);
