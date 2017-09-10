set spark.sql.shuffle.partitions=7;

select count(*) from aggrAdImpressions;

select count(*) from adImpressionStream;

select count(*) AS adCount, geo from aggrAdImpressions group by geo order by adCount desc limit 20;

select sum(uniques) AS totalUniques, geo from aggrAdImpressions where publisher='publisher11' group by geo order by totalUniques desc limit 20;

select count(*) AS adCount, geo from aggrAdImpressions group by geo order by adCount desc limit 20 with error 0.20 confidence 0.95;

select sum(uniques) AS totalUniques, geo from aggrAdImpressions where publisher='publisher11' group by geo order by totalUniques desc limit 20 with error 0.20 confidence 0.95;

select sum(uniques) AS totalUniques, geo from sampledAdImpressions where publisher='publisher11' group by geo order by totalUniques desc;

select count(*) as sample_cnt from sampledAdImpressions;

