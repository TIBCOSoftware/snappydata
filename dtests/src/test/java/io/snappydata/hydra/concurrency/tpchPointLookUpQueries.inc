io.snappydata.hydra.cluster.SnappyPrms-pointLookUpQueryList =
" select sum(l_quantity) from lineitem where l_orderkey = 8996"
" select min(l_discount) from lineitem where l_orderkey = 9254"
" select l_returnflag, count(*) from lineitem where l_orderkey = 1607 group by l_returnflag"
" select l_suppkey, count(*) from lineitem where l_orderkey = 15202 group by l_suppkey"
" select l_linenumber, l_suppkey from lineitem where l_orderkey = 16709 and l_partkey = 317"
" select * from orders where o_orderkey = 993"
" select * from orders where o_orderkey = 1350 and o_orderstatus = 'F'"
" select ps_suppkey, count(*) from partsupp where ps_partkey = 120 group by ps_suppkey"
" select count(*) from partsupp where ps_partkey = 250 and ps_comment = 'es. express frets nag. pending packages haggle carefully across the final instructions. quickly ironic'"
" select * from part where p_partkey = 778 and p_brand = 'Brand#32'"
" select * from supplier where s_suppkey = 50"
/*" select * from region where r_regionkey = 1 and r_name = 'AMERICA'"*/
" select * from nation where n_nationkey = 8 and n_name = 'INDIA'"
;