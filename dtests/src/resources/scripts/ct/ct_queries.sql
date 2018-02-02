elapsedtime on;
set spark.sql.shuffle.partitions=29;

select count(*) from ORDERS_DETAILS;

select max(single_order_did) from ORDERS_DETAILS;

select min(single_order_did) from ORDERS_DETAILS;

select AVG(single_order_did) from ORDERS_DETAILS;

select avg(single_order_did),min(single_order_did),max(single_order_did) from ORDERS_DETAILS;

select SRC_SYS,count(*) from ORDERS_DETAILS group by SRC_SYS;

select AVG(BID_PRICE),SRC_SYS from ORDERS_DETAILS GROUP BY SRC_SYS;

select SUM(TOTAL_EXECUTED_QTY),SRC_SYS from ORDERS_DETAILS GROUP BY SRC_SYS;

select SUM(TOTAL_EXECUTED_QTY),MIN(TOTAL_EXECUTED_QTY),MAX(TOTAL_EXECUTED_QTY),SRC_SYS from ORDERS_DETAILS
     WHERE SRC_SYS='APFF' GROUP BY SRC_SYS;

select count(*) from ORDERS_DETAILS where Src_sys='OATC';

select '5-CTFIX_ORDER' as SrcFl, * from ORDERS_DETAILS a , ORDERS_DETAILS b
   where a.glb_root_order_id = b.glb_root_order_id
   and a.trd_date >='20160413' and b.trd_date >='20160413'
   and b.src_sys='CRIO' order by a.glb_root_order_id, a.trd_datE;

select '4-CTFIX_ORDER' as SrcFl, a.glb_root_order_id, a.src_sys, count(*)
    from ORDERS_DETAILS a , ORDERS_DETAILS b
    where a.glb_root_order_id = b.glb_root_order_id and a.trd_date ='20160413'
    and b.trd_date ='20160413' and b.src_sys ='CRIO'
    group by a.glb_root_order_id, a.src_sys order by a.glb_root_order_id, a.src_sys;

select '3-CTFIX_ORDER' as SrcFl, * from ORDERS_DETAILS where trd_date='20160413' and src_sys='CRIO';

select '3-CTFIX_ORDER' as SrcFl, * from ORDERS_DETAILS where trd_date='20160413' and src_sys='CRIO' order by trd_date;

select '5-CTFIX_ORDER' as SrcFl, * from ORDERS_DETAILS
    where trd_date>='20160413' and glb_root_order_id in
    (select glb_root_order_id from ORDERS_DETAILS where trd_date>='20160413' and
     src_sys='CRIO' ) order by glb_root_order_id, trd_datE;

select '4-CTFIX_ORDER' as SrcFl, glb_root_order_id, src_sys, count(*)
    from ORDERS_DETAILS where trd_date='20160413'
    and glb_root_order_id in
    (select glb_root_order_id from ORDERS_DETAILS where trd_date='20160413' and src_sys='CRIO')
    group by glb_root_order_id, src_sys order by glb_root_order_id, src_sys;

select Event_type_cd, count(1) from ORDERS_DETAILS
    where TRD_DATE between '20160401' and '20160431' group by Event_type_cd limit 1000;

SELECT event_type_cd, src_sys FROM ORDERS_DETAILS WHERE TRD_DATE = '20160416' AND
    sys_order_stat_cd is NULL limit 1000;

SELECT ESOD.EXEC_INSTR, count(*) FROM ORDERS_DETAILS ESOD
    WHERE ESOD.TRD_DATE = '20160413' AND ESOD.EVENT_TYPE_CD = 'NEW_CONF'
    AND ESOD.EXEC_INSTR like '%A%' GROUP BY ESOD.EXEC_INSTR;

select EVENT_RCV_TS, EVENT_TS, src_sys,glb_root_src_sys_id,glb_root_order_id,
    ticker_symbol,SIDE,order_qty,EVENT_TYPE_CD,product_cat_cd,cntry_cd
    from ORDERS_DETAILS where trd_date > '20160212' and src_sys='CAIQS' and event_ts not like '%
    .%' limit 100;

select event_type_cd,event_rcv_ts,event_ts,sent_ts from ORDERS_DETAILS
    where trd_date='20160413' and glb_root_order_id='15344x8c7' and sys_order_id='20151210.92597';

select count(*) from EXEC_DETAILS a LEFT JOIN ORDERS_DETAILS b using(sys_root_order_id);

select distinct FLOW_CLASS from ORDERS_DETAILS;
