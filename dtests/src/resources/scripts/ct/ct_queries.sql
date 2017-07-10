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

-- (select TRD_DATE, ROOT_FLOW_CAT, sum(Notional) as notional, count(*) as trades, sum(shares) as shares
--   from (select execs.sys_order_id, execs.EXECUTED_QTY * execs.EXEC_PRICE as notional, execs.EXECUTED_QTY as shares,
--     execs.TRD_DATE, case when coalesce(root_exec.flow_cat,root.flow_cat) is null then 'UNKNOWN'
--    else coalesce(root_exec.flow_cat,root.flow_cat) end as ROOT_FLOW_CAT
--    from EXEC_DETAILS as execs
--    left join
--      (select distinct TRD_DATE,glb_root_order_id,flow_cat from EXEC_DETAILS
--        where TRD_DATE in ('20160325','20160413' )
--        and (PRODUCT_CAT_CD is null or PRODUCT_CAT_CD not in ('OPT','FUT','MLEG'))
--        and (exec_price_curr_cd = 'USD' OR exec_price_curr_cd is null)
--        and sys_src_sys_id in ('93', '7', '70', '115' ,'6','150','189','31','157','185','7','153','163133','80','51','139','137')
--        and sys_order_id = glb_root_order_id and sys_src_sys_id = glb_root_src_sys_id )
--    root_exec on execs.trd_date=root_exec.trd_date and execs.glb_root_order_id=root_exec.glb_root_order_id
--    left join
--      (select distinct TRD_DATE, glb_root_order_id,flow_cat from ORDERS_DETAILS T
--        where T.sys_order_id = T.glb_root_order_id and T.sys_src_sys_id = T.glb_root_src_sys_id
--        and T.sys_src_sys_id in ('93', '7', '70', '115' ,'6','150','189','31','157','185','7','153','163133','80','51','139','137')
--        and T.TRD_DATE in ('20160325','20160413' ) and (T.CURR_CD = 'USD' or T.CURR_CD is null)
--        and (T.PRODUCT_CAT_CD is null or T.PRODUCT_CAT_CD not in ('OPT', 'FUT','MLEG')) )
--    root on execs.trd_date=root.trd_date and execs.glb_root_order_id=root.glb_root_order_id
--      where execs.LEAF_EXEC_FG = 'Y' and execs.event_type_cd = 'FILLED_CONF'
--      and execs.sys_src_sys_id in ('93', '7', '70', '115' ,'6','150','189','31','157','185','7','153','163133','80','51','139','137')
--      and execs.SYS_ORDER_STAT_CD in ('2','1') and execs.TRD_DATE in ('20160325','20160413' )
--      and (execs.PRODUCT_CAT_CD is null or execs.PRODUCT_CAT_CD not in ('OPT', 'FUT','MLEG'))
--      and (execs.exec_price_curr_cd = 'USD' or execs.exec_price_curr_cd = null) )
--    Aggregated group by TRD_DATE, ROOT_FLOW_CAT order by TRD_DATE )
--union all
--  (select TRD_DATE, ROOT_FLOW_CAT, sum(Notional) as notional, count(*) as trades, sum (shares) as shares
--    from (select execs.sys_order_id, execs.EXECUTED_QTY * execs.EXEC_PRICE as notional,
--    execs.EXECUTED_QTY as shares, execs.TRD_DATE, 'ALL' as ROOT_FLOW_CAT from EXEC_DETAILS as execs
--    left join
--    (select distinct TRD_DATE,glb_root_order_id,flow_cat from EXEC_DETAILS
--        where TRD_DATE in ('20160325','20160413' )
--        and (PRODUCT_CAT_CD is null or PRODUCT_CAT_CD not in ('OPT','FUT','MLEG'))
--        and (exec_price_curr_cd = 'USD' OR exec_price_curr_cd is null)
--        and sys_src_sys_id in ('93', '7', '70', '115' ,'6','150','189','31','157','185','7','153','163133','80','51','139','137')
--        and sys_order_id = glb_root_order_id and sys_src_sys_id = glb_root_src_sys_id )
--        root_exec on execs.trd_date=root_exec.trd_date and execs.glb_root_order_id=root_exec.glb_root_order_id
--      left join
--      (select distinct TRD_DATE, glb_root_order_id,flow_cat from ORDERS_DETAILS T
--         where T.sys_order_id = T.glb_root_order_id and T.sys_src_sys_id = T.glb_root_src_sys_id
--         and T.sys_src_sys_id in ('93', '7', '70', '115' ,'6','150','189','31','157','185','7','153','163133','80','51','139','137')
--         and T.TRD_DATE in ('20160325','20160413') and (T.CURR_CD = 'USD' or T.CURR_CD is null)
--         and (T.PRODUCT_CAT_CD is null or T.PRODUCT_CAT_CD not in ('OPT', 'FUT','MLEG')) )
--      root on execs.trd_date=root.trd_date and execs.glb_root_order_id=root.glb_root_order_id
--      where execs.LEAF_EXEC_FG = 'Y' and execs.event_type_cd = 'FILLED_CONF'
--        and execs.sys_src_sys_id in ('93', '7', '70', '115' ,'6','150','189','31','157','185','7','153','163133','80','51','139','137')
--        and execs.SYS_ORDER_STAT_CD in ('2','1') and execs.TRD_DATE in ('20160325','20160413' )
--        and (execs.PRODUCT_CAT_CD is null or execs.PRODUCT_CAT_CD not in ('OPT', 'FUT','MLEG'))
--        and (execs.exec_price_curr_cd = 'USD' or execs.exec_price_curr_cd = null) )
--    Aggregated group by TRD_DATE, ROOT_FLOW_CAT order by TRD_DATE );

select distinct FLOW_CLASS from ORDERS_DETAILS;
