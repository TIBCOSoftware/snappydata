io.snappydata.hydra.testDMLOps.SnappySchemaPrms-tablesList = "app.mbl_test_scd";

io.snappydata.hydra.testDMLOps.SnappySchemaPrms-dataFileLocation = "/expo/shared/QA_DATA/slackData";
/*io.snappydata.hydra.testDMLOps.SnappySchemaPrms-dataFileLocation = "/data/snappyHydraLogs/slackData";*/

io.snappydata.hydra.testDMLOps.SnappySchemaPrms-csvFileNames = "slackIssues.csv";

io.snappydata.hydra.testDMLOps.SnappySchemaPrms-loadDataInParts=false;


io.snappydata.hydra.testDMLOps.SnappySchemaPrms-dmlTables =
"app.mbl_test_scd"
"app.oe_stream_comp_data_for_mbl"
;

io.snappydata.hydra.testDMLOps.SnappySchemaPrms-csvLocationforLargeData = "/export/shared/QA_DATA/slackData";
/*io.snappydata.hydra.testDMLOps.SnappySchemaPrms-csvLocationforLargeData = "/data/snappyHydraLogs/slackData";*/

io.snappydata.hydra.testDMLOps.SnappySchemaPrms-insertCsvFileNames =
"test_table.csv"
"oe_stream_comp_data_for_mbl.csv"
;

io.snappydata.hydra.testDMLOps.SnappySchemaPrms-insertStmts =
"insert into app.mbl_test_scd(ID,DATEKEY,CHECKIN_DATE,CHECKOUT_DATE,CRAWL_TIME,BATCH,SOURCE,IS_HIGH_STAR,MT_POI_ID,MT_ROOM_ID,MT_BREAKFAST,MT_GOODS_ID ,MT_BD_ID,MT_GOODS_VENDOR_ID,MT_BUSINESS_TYPE,MT_ROOM_STATUS,MT_POI_UV,MT_PRICE1,MT_PRICE2,MT_PRICE3,MT_PRICE4,MT_PRICE5,MT_PRICE6,MT_PRICE7,MT_PRICE8,MT_FLAG1,MT_FLAG2,MT_FLAG3,COMP_SITE_ID,COMP_POI_ID ,COMP_ROOM_ID ,COMP_BREAKFAST,COMP_GOODS_ID ,COMP_GOODS_VENDOR ,COMP_ROOM_STATUS,COMP_IS_PROMOTION,COMP_PAY_TYPE,COMP_GOODS_TYPE,COMP_PRICE1,COMP_PRICE2,COMP_PRICE3,COMP_PRICE4,COMP_PRICE5,COMP_PRICE6,COMP_PRICE7,COMP_PRICE8,COMP_FLAG1,COMP_FLAG2,COMP_FLAG3,VALID_STATUS,GMT_TIME,VERSION) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
"insert into app.oe_stream_comp_data_for_mbl(ID,DATEKEY,CHECKIN_DATE,CHECKOUT_DATE,CRAWL_TIME,BATCH,SOURCE,IS_HIGH_STAR,MT_POI_ID,MT_ROOM_ID,MT_BREAKFAST,MT_GOODS_ID ,MT_BD_ID,MT_GOODS_VENDOR_ID,MT_BUSINESS_TYPE,MT_ROOM_STATUS,MT_POI_UV,MT_PRICE1,MT_PRICE2,MT_PRICE3,MT_PRICE4,MT_PRICE5,MT_PRICE6,MT_PRICE7,MT_PRICE8,MT_FLAG1,MT_FLAG2,MT_FLAG3,COMP_SITE_ID,COMP_POI_ID ,COMP_ROOM_ID ,COMP_BREAKFAST,COMP_GOODS_ID ,COMP_GOODS_VENDOR ,COMP_ROOM_STATUS,COMP_IS_PROMOTION,COMP_PAY_TYPE,COMP_GOODS_TYPE,COMP_PRICE1,COMP_PRICE2,COMP_PRICE3,COMP_PRICE4,COMP_PRICE5,COMP_PRICE6,COMP_PRICE7,COMP_PRICE8,COMP_FLAG1,COMP_FLAG2,COMP_FLAG3,VALID_STATUS,GMT_TIME,VERSION,INTERVAL_DAYS,REAL_BATCH,START_TIME_LONG,END_TIME_LONG,START_TIME,END_TIME,START_REAL_BATCH,END_REAL_BATCH,FLAG,INSERT_TIME) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
;
